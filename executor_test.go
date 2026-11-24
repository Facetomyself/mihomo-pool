package mihomopool

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeUpstream 是一个最小的 HTTP 代理：收到请求直接把目标改写成本地 echo。
//
// 用来验证"请求确实走了某个代理"，不依赖任何外网。
type fakeUpstream struct {
	srv      *httptest.Server
	hits     atomic.Int64
	tag      string
	failNext atomic.Int64 // >0 时返回 502，模拟坏节点
	inflight atomic.Int64
	maxSeen  atomic.Int64
}

func newFakeUpstream(t *testing.T, tag string) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{tag: tag}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.hits.Add(1)
		cur := f.inflight.Add(1)
		defer f.inflight.Add(-1)
		for {
			old := f.maxSeen.Load()
			if cur <= old || f.maxSeen.CompareAndSwap(old, cur) {
				break
			}
		}
		if f.failNext.Load() > 0 {
			f.failNext.Add(-1)
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("bad gateway from " + f.tag))
			return
		}
		w.Header().Set("X-Upstream", f.tag)
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "hello via %s", f.tag)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeUpstream) proxyURL() string {
	u, _ := url.Parse(f.srv.URL)
	return "http://127.0.0.1:" + u.Port()
}

// targetSrv 是最终目标服务器。
func targetSrv(t *testing.T) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "target-ok")
	}))
	t.Cleanup(s.Close)
	return s
}

// newTestPool 构造一个不启动内核、只装节点的 Pool。
func newTestPool(t *testing.T, nodes []*Node, strategy Strategy) *Pool {
	t.Helper()
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	p := &Pool{
		opts:   o,
		kernel: newKernel(o),
		ctl:    newControl(o),
		nodes:  newNodeSet(),
	}
	d, err := NewDispatcher(strategy)
	if err != nil {
		t.Fatalf("NewDispatcher: %v", err)
	}
	p.disp.Store(d)
	p.exec = newExecutor(p)
	t.Cleanup(func() { p.exec.Close() })
	p.nodes.Replace(nodes)
	return p
}

func proxyNode(name, proxyURL string) *Node {
	return &Node{Name: name, Type: "http", ProxyURL: proxyURL, Port: 1, Alive: true, DelayMS: 50}
}

func TestExecutorRoutesThroughNode(t *testing.T) {
	up := newFakeUpstream(t, "A")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("A", up.proxyURL())}, StrategyLeastInflight)

	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL})
	if !resp.OK() {
		t.Fatalf("请求失败: %v", resp.Err)
	}
	if resp.Node != "A" {
		t.Fatalf("节点归属不对: %s", resp.Node)
	}
	if string(resp.Body) != "hello via A" {
		t.Fatalf("响应正文不对: %q", resp.Body)
	}
	if up.hits.Load() != 1 {
		t.Fatalf("代理命中次数不对: %d", up.hits.Load())
	}
}

func TestExecutorRetriesOnNextNode(t *testing.T) {
	bad := newFakeUpstream(t, "bad")
	bad.failNext.Store(100)
	good := newFakeUpstream(t, "good")
	tgt := targetSrv(t)

	p := newTestPool(t, []*Node{
		proxyNode("bad", bad.proxyURL()),
		proxyNode("good", good.proxyURL()),
	}, StrategyLeastInflight)

	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL, Retries: 3})
	if !resp.OK() {
		t.Fatalf("重试后应成功，得到 %v", resp.Err)
	}
	if resp.Node != "good" {
		t.Fatalf("最终应落在 good，得到 %s", resp.Node)
	}
	if resp.Attempts < 2 {
		t.Fatalf("应至少尝试 2 次，得到 %d", resp.Attempts)
	}

	// 坏节点应被记账：单次失败只降权不算死，连续失败才踢出候选
	badNode, _ := p.Nodes().Get("bad")
	if badNode.failCount.Load() == 0 {
		t.Fatal("坏节点未记录失败")
	}
	if goodNode, _ := p.Nodes().Get("good"); goodNode.failCount.Load() != 0 {
		t.Fatalf("好节点不应有失败记录: %d", goodNode.failCount.Load())
	}
	if badNode.Score() <= goodNode2Score(p, "good") {
		t.Fatalf("坏节点分数未降权: bad=%d", badNode.Score())
	}
	badNode.failCount.Store(3)
	if badNode.Healthy() {
		t.Fatal("连续失败 3 次后不应判定为健康")
	}
}

func TestExecutorPerNodeCap(t *testing.T) {
	up := newFakeUpstream(t, "single")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("single", up.proxyURL())}, StrategyLeastInflight)
	p.ConfigureExecutor(8, 2) // 单节点最多 2 在飞，8 个 worker

	var wg sync.WaitGroup
	var succeeds, capped int64
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL, Retries: 0})
			if resp.OK() {
				atomic.AddInt64(&succeeds, 1)
				return
			}
			if resp.Err != nil && strings.Contains(resp.Err.Error(), "在飞已满") {
				atomic.AddInt64(&capped, 1)
			}
		}()
	}
	wg.Wait()

	if succeeds == 0 {
		t.Fatal("一个都没成功")
	}

	// 真正要保证的是硬上限：任何时刻单节点在飞数不超过 PerNode。
	// 不能用"必须出现限流"来断言——CI 机器够快时 40 个请求可能串行跑完，
	// 从未撞到上限，那是健康表现而不是失败。
	// 这里只要求：一旦发生过限流，就说明上限确实在生效；
	// 且实测并发峰值永远不能超过配置值。
	if max := up.maxSeen.Load(); max > 2 {
		t.Fatalf("单节点并发超过上限 2，实测峰值 %d", max)
	}
	if capped > 0 && succeeds == 0 {
		t.Fatalf("全部被限流（成功 %d，被限 %d），上限过严", succeeds, capped)
	}
	t.Logf("成功 %d，被限流 %d，实测峰值 %d", succeeds, capped, up.maxSeen.Load())
}

func TestExecutorConcurrentManyRequests(t *testing.T) {
	var ups []*fakeUpstream
	var nodes []*Node
	for i := 0; i < 5; i++ {
		u := newFakeUpstream(t, "n"+strconv.Itoa(i))
		ups = append(ups, u)
		nodes = append(nodes, proxyNode("n"+strconv.Itoa(i), u.proxyURL()))
	}
	tgt := targetSrv(t)
	p := newTestPool(t, nodes, StrategyLeastInflight)
	p.ConfigureExecutor(16, 8)

	const total = 120
	reqs := make([]*Request, 0, total)
	for i := 0; i < total; i++ {
		reqs = append(reqs, &Request{URL: tgt.URL, Tag: strconv.Itoa(i)})
	}
	ch, err := p.Executor().RunAsync(context.Background(), reqs)
	if err != nil {
		t.Fatalf("RunAsync: %v", err)
	}

	seen := map[int]bool{}
	var ok, fail int
	for resp := range ch {
		idx, _ := strconv.Atoi(resp.Tag)
		seen[idx] = true
		if resp.OK() {
			ok++
		} else {
			fail++
		}
	}
	if fail != 0 {
		t.Fatalf("有 %d 个请求失败", fail)
	}
	if ok != total || len(seen) != total {
		t.Fatalf("结果不完整: ok=%d seen=%d", ok, len(seen))
	}

	// 负载应分散到多个节点
	used := 0
	for _, u := range ups {
		if u.hits.Load() > 0 {
			used++
		}
	}
	if used < 3 {
		t.Fatalf("负载分散不足，只用到 %d 个节点", used)
	}
}

func TestExecutorForEachCollects(t *testing.T) {
	up := newFakeUpstream(t, "A")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("A", up.proxyURL())}, StrategyLeastInflight)

	reqs := []*Request{
		{URL: tgt.URL, Tag: "1"},
		{URL: tgt.URL, Tag: "2"},
		{URL: tgt.URL, Tag: "3"},
	}
	var got []string
	err := p.Executor().ForEach(context.Background(), reqs, func(r *Response) {
		if r.OK() {
			got = append(got, r.Tag)
		}
	})
	if err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("期望 3 个结果，得到 %d", len(got))
	}
}

func TestExecutorNoNodesNoDirect(t *testing.T) {
	tgt := targetSrv(t)
	p := newTestPool(t, nil, StrategyLeastInflight)
	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL})
	if resp.OK() {
		t.Fatal("无节点且未允许直连，不应成功")
	}
	if resp.Err == nil {
		t.Fatal("应有错误")
	}
}

func TestExecutorDirectFallback(t *testing.T) {
	tgt := targetSrv(t)
	p := newTestPool(t, nil, StrategyLeastInflight)
	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL, AllowDirect: true, Retries: 0})
	if !resp.OK() {
		t.Fatalf("直连兜底失败: %v", resp.Err)
	}
	if resp.Node != "direct" {
		t.Fatalf("节点应为 direct，得到 %s", resp.Node)
	}
	if string(resp.Body) != "target-ok" {
		t.Fatalf("正文不对: %q", resp.Body)
	}
}

func TestExecutorBodyTruncation(t *testing.T) {
	big := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 5000)))
	}))
	defer big.Close()

	p := newTestPool(t, nil, StrategyLeastInflight)
	resp := p.Executor().Do(context.Background(), &Request{URL: big.URL, AllowDirect: true, MaxBodyBytes: 100})
	if !resp.OK() {
		t.Fatalf("请求失败: %v", resp.Err)
	}
	if len(resp.Body) != 100 {
		t.Fatalf("正文未截断: %d", len(resp.Body))
	}
	if !resp.Truncated {
		t.Fatal("Truncated 未置位")
	}
}

func TestExecutorHashKeyPinsNode(t *testing.T) {
	var nodes []*Node
	var ups []*fakeUpstream
	for i := 0; i < 4; i++ {
		u := newFakeUpstream(t, "h"+strconv.Itoa(i))
		ups = append(ups, u)
		nodes = append(nodes, proxyNode("h"+strconv.Itoa(i), u.proxyURL()))
	}
	tgt := targetSrv(t)
	p := newTestPool(t, nodes, StrategyHash)

	var first string
	for i := 0; i < 20; i++ {
		resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL, HashKey: "same-key"})
		if !resp.OK() {
			t.Fatalf("请求失败: %v", resp.Err)
		}
		if i == 0 {
			first = resp.Node
			continue
		}
		if resp.Node != first {
			t.Fatalf("同 key 未固定到同一节点: %s vs %s", first, resp.Node)
		}
	}
}

func TestExecutorFilterLimitsNodes(t *testing.T) {
	a := newFakeUpstream(t, "groupA")
	b := newFakeUpstream(t, "groupB")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{
		{Name: "a", Type: "http", Group: "A", ProxyURL: a.proxyURL(), Port: 1, Alive: true, DelayMS: 50},
		{Name: "b", Type: "http", Group: "B", ProxyURL: b.proxyURL(), Port: 1, Alive: true, DelayMS: 50},
	}, StrategyLeastInflight)

	for i := 0; i < 10; i++ {
		resp := p.Executor().Do(context.Background(), &Request{
			URL: tgt.URL, Filter: NodeFilter{Groups: []string{"B"}},
		})
		if !resp.OK() {
			t.Fatalf("请求失败: %v", resp.Err)
		}
		if resp.Node != "b" {
			t.Fatalf("筛选失效，落到 %s", resp.Node)
		}
	}
	if a.hits.Load() != 0 {
		t.Fatalf("被筛掉的节点仍被命中 %d 次", a.hits.Load())
	}
}

func TestExecutorSubmitAndClose(t *testing.T) {
	up := newFakeUpstream(t, "A")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("A", up.proxyURL())}, StrategyLeastInflight)

	ch, err := p.Executor().Submit(context.Background(), &Request{URL: tgt.URL})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	select {
	case resp := <-ch:
		if !resp.OK() {
			t.Fatalf("请求失败: %v", resp.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Submit 超时")
	}

	p.Executor().Close()
	if _, err := p.Executor().Submit(context.Background(), &Request{URL: tgt.URL}); err == nil {
		t.Fatal("关闭后 Submit 应报错")
	}
	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL})
	if resp.Err == nil {
		t.Fatal("关闭后 Do 应报错")
	}
}

func TestExecutorContextCancel(t *testing.T) {
	// 目标端故意慢，验证 ctx 取消能及时返回
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		w.WriteHeader(200)
	}))
	defer slow.Close()

	p := newTestPool(t, nil, StrategyLeastInflight)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	resp := p.Executor().Do(ctx, &Request{URL: slow.URL, AllowDirect: true, Timeout: 5 * time.Second})
	if resp.Err == nil {
		t.Fatal("期望超时错误")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("取消不及时，耗时 %v", time.Since(start))
	}
}

func TestExecutorBadProxyURLRejected(t *testing.T) {
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{
		{Name: "bad", Type: "http", ProxyURL: "ftp://127.0.0.1:1", Port: 1, Alive: true, DelayMS: 50},
	}, StrategyLeastInflight)
	resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL, Retries: 0})
	if resp.Err == nil {
		t.Fatal("非法代理协议应报错")
	}
	if !strings.Contains(resp.Err.Error(), "不支持") {
		t.Fatalf("错误信息不含预期内容: %v", resp.Err)
	}
}

func TestExecutorStatsAndConfigure(t *testing.T) {
	up := newFakeUpstream(t, "A")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("A", up.proxyURL())}, StrategyLeastInflight)

	p.ConfigureExecutor(4, 1)
	st := p.Executor().Stats()
	if st.Workers != 4 {
		t.Fatalf("worker 数不对: %d", st.Workers)
	}
	if !p.Executor().Do(context.Background(), &Request{URL: tgt.URL}).OK() {
		t.Fatal("调整参数后应仍可用")
	}
}

func TestExecutorKeepAliveReuse(t *testing.T) {
	up := newFakeUpstream(t, "A")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{proxyNode("A", up.proxyURL())}, StrategyLeastInflight)

	// 同一 transport 应复用连接：串行发 10 次
	for i := 0; i < 10; i++ {
		if resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL}); !resp.OK() {
			t.Fatalf("第 %d 次失败: %v", i, resp.Err)
		}
	}
	if up.hits.Load() != 10 {
		t.Fatalf("命中次数不对: %d", up.hits.Load())
	}
	// 同一代理地址只应有一个 transport
	p.Executor().trMu.Lock()
	n := len(p.Executor().trs)
	p.Executor().trMu.Unlock()
	if n != 1 {
		t.Fatalf("transport 未复用，缓存 %d 个", n)
	}
}

func TestTransportCacheKeyedByProxy(t *testing.T) {
	a := newFakeUpstream(t, "A")
	b := newFakeUpstream(t, "B")
	tgt := targetSrv(t)
	p := newTestPool(t, []*Node{
		proxyNode("A", a.proxyURL()),
		proxyNode("B", b.proxyURL()),
	}, StrategyRoundRobin)

	for i := 0; i < 6; i++ {
		if resp := p.Executor().Do(context.Background(), &Request{URL: tgt.URL}); !resp.OK() {
			t.Fatalf("请求失败: %v", resp.Err)
		}
	}
	p.Executor().trMu.Lock()
	n := len(p.Executor().trs)
	p.Executor().trMu.Unlock()
	if n != 2 {
		t.Fatalf("期望 2 个 transport，得到 %d", n)
	}
	if a.hits.Load() == 0 || b.hits.Load() == 0 {
		t.Fatal("两个节点都应被用到")
	}
}

func TestPortOpenOnClosedPort(t *testing.T) {
	// 拿一个几乎不可能被占用的高位端口
	if portOpen(1) {
		t.Fatal("端口 1 不应开放")
	}
}

func TestRingBufferKeepsTail(t *testing.T) {
	r := newRingBuffer(3)
	for i := 0; i < 10; i++ {
		_, _ = r.Write([]byte("line" + strconv.Itoa(i) + "\n"))
	}
	lines := r.lines(10)
	if len(lines) != 3 {
		t.Fatalf("环形缓冲应只保留 3 行，得到 %d", len(lines))
	}
	if lines[2] != "line9" {
		t.Fatalf("尾部内容不对: %v", lines)
	}
}

func TestNetJoinHostPortV6(t *testing.T) {
	if got := net.JoinHostPort("::1", "80"); got != "[::1]:80" {
		t.Fatalf("IPv6 拼接不对: %s", got)
	}
}

// goodNode2Score 取某节点的当前分数，用于比较。
func goodNode2Score(p *Pool, name string) int64 {
	if n, ok := p.Nodes().Get(name); ok {
		return n.Score()
	}
	return 0
}
