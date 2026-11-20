//go:build embedded

package mihomopool

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// 内嵌后端的测试。
//
// 与进程后端不同，这里不需要外部 mihomo 二进制，
// 所以这些用例在任何环境都能跑——这正是内嵌方案的价值。

func newEmbedTestPool(t *testing.T) *Pool {
	t.Helper()
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.SocksPort = freePort(t)
	o.APIPort = freePort(t)
	o.BasePort = freePort(t)
	t.Logf("测试分配端口: socks=%d api=%d base=%d", o.SocksPort, o.APIPort, o.BasePort)
	o.StartTimeout = 40 * time.Second
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	p, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p
}

func TestEmbeddedBackendName(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	_ = o.normalize()
	b, err := newBackend(o)
	if err != nil {
		t.Fatalf("newBackend: %v", err)
	}
	if b.Name() != "embedded" {
		t.Fatalf("后端名应为 embedded，得到 %q", b.Name())
	}
	if b.PID() != os.Getpid() {
		t.Fatalf("嵌入模式 PID 应为本进程 %d，得到 %d", os.Getpid(), b.PID())
	}
}

// TestEmbeddedNoExternalBinary 是内嵌方案的核心价值：
// 没有 mihomo 二进制也能起内核。
func TestEmbeddedNoExternalBinary(t *testing.T) {
	// 故意把搜索链指向不存在的路径
	t.Setenv("MIHOMO_POOL_BINARY", "")

	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.Binary = "/nonexistent/mihomo"
	o.SocksPort = freePort(t)
	o.APIPort = freePort(t)
	o.BasePort = freePort(t)
	t.Logf("测试分配端口: socks=%d api=%d base=%d", o.SocksPort, o.APIPort, o.BasePort)
	o.StartTimeout = 40 * time.Second
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	p, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := []NodeSpec{{
		Name: "dummy", Type: "socks5",
		Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}
	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("内嵌启动失败（不该需要外部二进制）: %v", err)
	}
	defer func() {
		sctx, sc := context.WithTimeout(context.Background(), 20*time.Second)
		defer sc()
		_ = p.Stop(sctx)
	}()

	st := p.Status(ctx)
	if st.State != StateRunning {
		t.Fatalf("状态不是 running: %+v", st)
	}
	if st.Version == "" {
		t.Fatal("未取得内核版本")
	}
	if st.Ports != 1 {
		t.Fatalf("独立端口数不对: %d", st.Ports)
	}
}

// TestEmbeddedGeoDataExtracted 验证 geodata 被释放到工作目录。
func TestEmbeddedGeoDataExtracted(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	_ = o.normalize()
	b, _ := newBackend(o)
	eb := b.(*embedBackend)

	dir, err := eb.prepareGeoData()
	if err != nil {
		t.Fatalf("prepareGeoData: %v", err)
	}

	for _, name := range []string{"geoip.metadb", "GeoSite.dat"} {
		p := filepath.Join(dir, name)
		st, err := os.Stat(p)
		if err != nil {
			t.Fatalf("geodata %s 未释放: %v", name, err)
		}
		if st.Size() < 100*1024 {
			t.Fatalf("geodata %s 体积异常: %d", name, st.Size())
		}
	}

	// 二次调用应复用（大小一致就跳过重写）
	before, _ := os.Stat(filepath.Join(dir, "geoip.metadb"))
	time.Sleep(10 * time.Millisecond)
	if _, err := eb.prepareGeoData(); err != nil {
		t.Fatalf("二次 prepareGeoData: %v", err)
	}
	after, _ := os.Stat(filepath.Join(dir, "geoip.metadb"))
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatal("geodata 被重复写入（应该按大小跳过）")
	}
}

// TestEmbeddedGEOIPRuleLoads 验证 GEOIP/GEOSITE 规则能真正加载。
//
// 这是内嵌方案最容易踩的坑：没有 -d 工作目录时，
// GEOIP 规则会因为找不到 MMDB 而在 ParseRawConfig 阶段直接失败。
func TestEmbeddedGEOIPRuleLoads(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.SocksPort = freePort(t)
	o.APIPort = freePort(t)
	o.BasePort = freePort(t)
	t.Logf("测试分配端口: socks=%d api=%d base=%d", o.SocksPort, o.APIPort, o.BasePort)
	o.StartTimeout = 60 * time.Second
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	p, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	nodes := []NodeSpec{{
		Name: "n1", Type: "socks5", Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}
	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("启动失败（geoip 规则应能加载）: %v", err)
	}
	defer func() {
		sctx, sc := context.WithTimeout(context.Background(), 20*time.Second)
		defer sc()
		_ = p.Stop(sctx)
	}()

	if p.Status(ctx).Version == "" {
		t.Fatal("内核未正常工作")
	}
}

// TestEmbeddedStopReleasesPorts 验证 Stop 之后端口真的释放。
//
// executor.Shutdown 是异步的：如果不等它完成，立刻重启会撞
// "address already in use"。这是内嵌方案特有的坑。
func TestEmbeddedStopReleasesPorts(t *testing.T) {
	p := newEmbedTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	nodes := []NodeSpec{{
		Name: "n1", Type: "socks5", Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}
	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}

	opts := p.Options()
	if !portOpen(opts.BasePort) {
		t.Fatal("节点端口未监听")
	}

	sctx, sc := context.WithTimeout(context.Background(), 30*time.Second)
	defer sc()
	if err := p.Stop(sctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// Stop 返回时端口必须已经空了
	if portOpen(opts.BasePort) {
		t.Fatalf("Stop 之后节点端口 %d 仍在监听", opts.BasePort)
	}
	if portOpen(opts.APIPort) {
		t.Fatalf("Stop 之后控制面端口 %d 仍在监听", opts.APIPort)
	}
}

// TestEmbeddedRestartAfterStop 验证停掉之后能再起来（端口冲突回归）。
func TestEmbeddedRestartAfterStop(t *testing.T) {
	p := newEmbedTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	nodes := []NodeSpec{{
		Name: "n1", Type: "socks5", Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}

	for i := 0; i < 2; i++ {
		if err := p.Start(ctx, nodes, nil); err != nil {
			t.Fatalf("第 %d 次 Start 失败: %v", i+1, err)
		}
		sctx, sc := context.WithTimeout(context.Background(), 30*time.Second)
		if err := p.Stop(sctx); err != nil {
			sc()
			t.Fatalf("第 %d 次 Stop 失败: %v", i+1, err)
		}
		sc()
	}
}

// TestEmbeddedBackendStatus 验证状态里带后端名。
func TestEmbeddedBackendStatus(t *testing.T) {
	p := newEmbedTestPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodes := []NodeSpec{{
		Name: "n1", Type: "socks5", Server: "127.0.0.1", Port: 1,
		Raw: map[string]any{"server": "127.0.0.1", "port": 1},
	}}
	if err := p.Start(ctx, nodes, nil); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		sctx, sc := context.WithTimeout(context.Background(), 20*time.Second)
		defer sc()
		_ = p.Stop(sctx)
	}()

	if got := p.Kernel().BackendName(); got != "embedded" {
		t.Fatalf("BackendName 应为 embedded，得到 %q", got)
	}
}
