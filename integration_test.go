//go:build !embedded

package mihomopool

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 集成测试：需要真实 mihomo 二进制。
//
// 找不到内核时跳过（用 t.Skip 而不是空跑），
// 显式设置 MIHOMO_POOL_INTEGRATION=1 可强制要求跑通。

func requireMihomo(t *testing.T) string {
	t.Helper()
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	b, berr := newBackend(o)
	if berr != nil {
		t.Skipf("跳过：没有可用的内核后端（%v）", berr)
	}
	pb, ok := b.(*processBackend)
	if !ok {
		t.Skip("跳过：当前是无外部二进制后端")
	}
	bin, err := pb.resolveBinary()
	if err != nil {
		if os.Getenv("MIHOMO_POOL_INTEGRATION") == "1" {
			t.Fatalf("集成测试要求 mihomo 内核: %v", err)
		}
		t.Skipf("跳过：未找到 mihomo 内核（%v）", err)
	}
	return bin
}

// fakeSocks 启动一个极简 SOCKS5 服务器，记录被命中次数。
//
// 只实现无认证 CONNECT：mihomo 把节点数据发到目标明文前会先做 SOCKS 握手，
// 所以这里完整解出目标地址并建立到目标的连接即可。
func fakeSocks(t *testing.T, tag string) (addr string, hits *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("socks 监听失败: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	hits = &atomic.Int64{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(20 * time.Second))

				// 方法协商：5 1 0 → 5 0
				head := make([]byte, 2)
				if _, err := readFull(c, head); err != nil {
					return
				}
				if head[0] != 5 {
					return
				}
				methods := make([]byte, int(head[1]))
				if _, err := readFull(c, methods); err != nil {
					return
				}
				if _, err := c.Write([]byte{5, 0}); err != nil {
					return
				}

				// 请求：5 CMD RSV ATYP ADDR PORT
				hdr := make([]byte, 4)
				if _, err := readFull(c, hdr); err != nil {
					return
				}
				if hdr[1] != 1 { // 只支持 CONNECT
					return
				}
				var host string
				switch hdr[3] {
				case 1:
					b := make([]byte, 4)
					if _, err := readFull(c, b); err != nil {
						return
					}
					host = net.IP(b).String()
				case 3:
					l := make([]byte, 1)
					if _, err := readFull(c, l); err != nil {
						return
					}
					b := make([]byte, int(l[0]))
					if _, err := readFull(c, b); err != nil {
						return
					}
					host = string(b)
				case 4:
					b := make([]byte, 16)
					if _, err := readFull(c, b); err != nil {
						return
					}
					host = net.IP(b).String()
				default:
					return
				}
				pb := make([]byte, 2)
				if _, err := readFull(c, pb); err != nil {
					return
				}
				port := int(pb[0])<<8 | int(pb[1])

				hits.Add(1)

				target, err := net.DialTimeout("tcp", net.JoinHostPort(host, strconv.Itoa(port)), 5*time.Second)
				if err != nil {
					_, _ = c.Write([]byte{5, 5, 0, 1, 0, 0, 0, 0, 0, 0})
					return
				}
				defer target.Close()
				if _, err := c.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}); err != nil {
					return
				}

				var wg sync.WaitGroup
				wg.Add(2)
				go func() { defer wg.Done(); _, _ = copyAll(target, c) }()
				go func() { defer wg.Done(); _, _ = copyAll(c, target) }()
				wg.Wait()
			}(conn)
		}
	}()
	return ln.Addr().String(), hits
}

func readFull(c net.Conn, b []byte) (int, error) {
	n := 0
	for n < len(b) {
		m, err := c.Read(b[n:])
		if err != nil {
			return n, err
		}
		n += m
	}
	return n, nil
}

func copyAll(dst net.Conn, src net.Conn) (int64, error) {
	buf := make([]byte, 32<<10)
	var total int64
	for {
		n, err := src.Read(buf)
		if n > 0 {
			m, werr := dst.Write(buf[:n])
			total += int64(m)
			if werr != nil {
				return total, werr
			}
		}
		if err != nil {
			return total, err
		}
	}
}

// TestIntegrationKernelLifecycle 真起内核、查状态、停掉。
func TestIntegrationKernelLifecycle(t *testing.T) {
	bin := requireMihomo(t)

	o := DefaultOptions()
	o.Binary = bin
	o.WorkDir = t.TempDir()
	o.SocksPort = freePort(t)
	o.APIPort = freePort(t)
	o.BasePort = 0
	o.BasePort = freePort(t)
	o.StartTimeout = 25 * time.Second
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	p, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	// 一个永远连不上的节点，只为让配置里有内容
	nodes := []NodeSpec{{
		Name: "dummy", Type: "socks5", Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}

	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
		defer sc()
		_ = p.Stop(sctx)
	})

	st := p.Status(ctx)
	if st.State != StateRunning {
		t.Fatalf("状态不是 running: %+v", st)
	}
	if st.Version == "" {
		t.Fatal("未取得内核版本")
	}
	if st.Nodes < 1 {
		t.Fatalf("未同步到节点: %d", st.Nodes)
	}
	if st.Ports != 1 {
		t.Fatalf("独立端口数不对: %d", st.Ports)
	}

	// 独立端口必须真的在监听
	if !portOpen(o.BasePort) {
		t.Fatalf("节点独立端口 %d 未监听", o.BasePort)
	}
	if !portOpen(o.SocksPort) || !portOpen(o.APIPort) {
		t.Fatal("共享端口或 API 端口未监听")
	}

	// 通过独立端口发一个请求：节点本身不可用，应拿到错误而不是超时挂死
	resp := p.Executor().Do(ctx, &Request{
		URL:     "http://example.com/",
		Timeout: 3 * time.Second,
		Retries: 0,
	})
	if resp.OK() {
		t.Log("节点居然可用（本机 1 端口通常不通，视环境而定）")
	} else if resp.Err == nil {
		t.Fatal("失败必须带错误")
	}
}

// TestIntegrationIsolatedPorts 验证两个节点各走各的端口、互不串味。
func TestIntegrationIsolatedPorts(t *testing.T) {
	bin := requireMihomo(t)

	// 目标服务器
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()
	targetPort := target.Listener.Addr().(*net.TCPAddr).Port

	// 两个假 SOCKS 节点，各自记录被命中次数
	var nodes []NodeSpec
	var hits []*atomic.Int64
	for i := 0; i < 2; i++ {
		addr, h := fakeSocks(t, fmt.Sprintf("n%d", i))
		host, portStr, _ := net.SplitHostPort(addr)
		pn := freePort(t)
		_ = pn
		nodes = append(nodes, NodeSpec{
			Name: fmt.Sprintf("node%d", i), Type: "socks5",
			Server: host, Port: mustAtoi(t, portStr),
			Raw: map[string]any{
				"server": host, "port": mustAtoi(t, portStr),
			},
		})
		hits = append(hits, h)
	}

	o := DefaultOptions()
	o.Binary = bin
	o.WorkDir = t.TempDir()
	o.SocksPort = freePort(t)
	o.APIPort = freePort(t)
	o.BasePort = freePort(t) // 要连续两个：BasePort, BasePort+1
	// 保证 BasePort+1 也空闲
	for portOpen(o.BasePort) || portOpen(o.BasePort+1) {
		o.BasePort = freePort(t)
	}
	o.StartTimeout = 25 * time.Second
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	p, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
		defer sc()
		_ = p.Stop(sctx)
	})
	// 20 个并发请求，单节点上限调到 20，避免"在飞已满"干扰隔离验证
	p.ConfigureExecutor(20, 20)

	// 两个节点端口都在监听
	for i := 0; i < 2; i++ {
		if !portOpen(o.BasePort + i) {
			t.Fatalf("节点 %d 端口 %d 未监听", i, o.BasePort+i)
		}
	}

	url0 := p.ProxyURLFor("node0")
	url1 := p.ProxyURLFor("node1")
	if url0 == "" || url1 == "" || url0 == url1 {
		t.Fatalf("节点端口未隔离: %q / %q", url0, url1)
	}

	// 并发打 20 个请求，全部指定走 node0，node1 不应被命中
	targetURL := fmt.Sprintf("http://127.0.0.1:%d/", targetPort)
	var wg sync.WaitGroup
	var fail int64
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := p.Executor().Do(ctx, &Request{
				URL:     targetURL,
				Timeout: 8 * time.Second,
				Retries: 1,
				Filter:  NodeFilter{NamePrefix: "node0"},
			})
			if !resp.OK() {
				atomic.AddInt64(&fail, 1)
				t.Logf("fail: node=%s status=%d err=%v", resp.Node, resp.Status, resp.Err)
			}
		}()
	}
	wg.Wait()

	if fail > 0 {
		t.Fatalf("%d/20 个请求失败（节点隔离或转发有问题）", fail)
	}
	if hits[0].Load() == 0 {
		t.Fatal("node0 完全没被命中")
	}
	if hits[1].Load() != 0 {
		t.Fatalf("node1 被串门命中 %d 次（端口未隔离）", hits[1].Load())
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		t.Fatalf("解析端口 %q 失败: %v", s, err)
	}
	return n
}

// TestIntegrationVersionBanner 只验证二进制能被调用。
func TestIntegrationVersionBanner(t *testing.T) {
	bin := requireMihomo(t)
	out, err := exec.Command(bin, "-v").CombinedOutput()
	if err != nil {
		t.Fatalf("mihomo -v 失败: %v", err)
	}
	if !strings.Contains(string(out), "Mihomo") && !strings.Contains(string(out), "mihomo") {
		t.Fatalf("版本输出不认识: %s", out)
	}
}
