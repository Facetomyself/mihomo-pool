package mihomopool

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// 订阅池测试用假订阅服务器，不依赖外部网络。

// newSubServer 起一个返回固定 Clash YAML 的订阅服务。
func newSubServer(t *testing.T, nodes string) *httptest.Server {
	t.Helper()
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/yaml")
		_, _ = fmt.Fprintf(w, "proxies:\n%s\n", nodes)
	}))
	t.Cleanup(s.Close)
	return s
}

func TestSubscriptionPoolAddValidation(t *testing.T) {
	p := NewSubscriptionPool(SubscriptionPoolOptions{})

	if err := p.Add(SubscriptionSpec{URL: "https://x"}); err == nil {
		t.Fatal("缺 Name 应报错")
	}
	if err := p.Add(SubscriptionSpec{Name: "a"}); err == nil {
		t.Fatal("缺 URL 应报错")
	}
	if err := p.Add(SubscriptionSpec{Name: "a", URL: "https://x", Quota: 1.5}); err == nil {
		t.Fatal("Quota 越界应报错")
	}
	if err := p.Add(SubscriptionSpec{Name: "a", URL: "https://x"}); err != nil {
		t.Fatalf("正常添加失败: %v", err)
	}
	if err := p.Add(SubscriptionSpec{Name: "a", URL: "https://y"}); err == nil {
		t.Fatal("重名应报错")
	}
}

func TestSubscriptionPoolRefreshAggregates(t *testing.T) {
	s1 := newSubServer(t, `
  - {name: "hk-1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}
  - {name: "hk-2", type: ss, server: b.example.com, port: 2, cipher: aes-128-gcm, password: y}`)
	s2 := newSubServer(t, `
  - {name: "jp-1", type: vless, server: c.example.com, port: 3, uuid: u1}`)

	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	if err := p.AddMany([]SubscriptionSpec{
		{Name: "airport-a", URL: s1.URL},
		{Name: "airport-b", URL: s2.URL},
	}); err != nil {
		t.Fatalf("AddMany: %v", err)
	}

	ok, fail := p.RefreshAll(context.Background(), true)
	if ok != 2 || fail != 0 {
		t.Fatalf("期望 2 成功 0 失败，得到 %d/%d", ok, fail)
	}

	nodes, err := p.Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(nodes) != 3 {
		t.Fatalf("期望 3 个节点，得到 %d", len(nodes))
	}

	// 每个节点都必须带归属
	bySub := map[string]int{}
	for _, n := range nodes {
		if n.Subscription == "" {
			t.Fatalf("节点 %s 缺少订阅归属", n.Name)
		}
		bySub[n.Subscription]++
	}
	if bySub["airport-a"] != 2 || bySub["airport-b"] != 1 {
		t.Fatalf("归属统计不对: %v", bySub)
	}
}

func TestSubscriptionPoolIsolatesFailures(t *testing.T) {
	good := newSubServer(t, `
  - {name: "n1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)

	p := NewSubscriptionPool(SubscriptionPoolOptions{MaxConsecutiveFail: 2})
	if err := p.AddMany([]SubscriptionSpec{
		{Name: "good", URL: good.URL},
		{Name: "bad", URL: "http://127.0.0.1:1/nonexistent"},
	}); err != nil {
		t.Fatalf("AddMany: %v", err)
	}

	ok, fail := p.RefreshAll(context.Background(), true)
	if ok != 1 || fail != 1 {
		t.Fatalf("期望 1 成功 1 失败，得到 %d/%d", ok, fail)
	}

	// 好订阅的节点必须仍在
	nodes, _ := p.Nodes()
	if len(nodes) != 1 || nodes[0].Subscription != "good" {
		t.Fatalf("好订阅的节点丢失: %v", nodes)
	}

	infos := p.Infos()
	for _, in := range infos {
		if in.Name == "bad" {
			if in.State != SubDegraded && in.State != SubCircuitOpen {
				t.Fatalf("坏订阅状态应降级/熔断，得到 %s", in.State)
			}
			if in.ConsecFail == 0 {
				t.Fatal("坏订阅未记录连续失败")
			}
		}
	}
}

func TestSubscriptionPoolCircuitBreaker(t *testing.T) {
	p := NewSubscriptionPool(SubscriptionPoolOptions{
		MaxConsecutiveFail: 2,
		CircuitHalfOpen:    time.Hour,
	})
	if err := p.Add(SubscriptionSpec{Name: "bad", URL: "http://127.0.0.1:1/x"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	for i := 0; i < 2; i++ {
		p.RefreshAll(context.Background(), true)
	}

	infos := p.Infos()
	if len(infos) != 1 {
		t.Fatalf("期望 1 个订阅信息，得到 %d", len(infos))
	}
	if infos[0].State != SubCircuitOpen {
		t.Fatalf("连续失败 2 次后应熔断，得到 %s", infos[0].State)
	}
	if infos[0].EffWeight != 0 {
		t.Fatalf("熔断订阅权重应为 0，得到 %v", infos[0].EffWeight)
	}

	// 熔断后 Nodes 不应贡献节点
	nodes, _ := p.Nodes()
	if len(nodes) != 0 {
		t.Fatalf("熔断订阅仍在贡献节点: %d", len(nodes))
	}
}

func TestSubscriptionPoolWeightAffectsOrder(t *testing.T) {
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	if err := p.AddMany([]SubscriptionSpec{
		{Name: "low", URL: "https://a/x", Weight: 1},
		{Name: "high", URL: "https://b/x", Weight: 5, Priority: 3},
	}); err != nil {
		t.Fatalf("AddMany: %v", err)
	}

	infos := p.Infos()
	var low, high float64
	for _, in := range infos {
		switch in.Name {
		case "low":
			low = in.EffWeight
		case "high":
			high = in.EffWeight
		}
	}
	if high <= low {
		t.Fatalf("高权重订阅的有效权重应更大: high=%v low=%v", high, low)
	}

	SortInfosByWeight(infos)
	if infos[0].Name != "high" {
		t.Fatalf("按权重排序后 high 应在首位，得到 %s", infos[0].Name)
	}
}

func TestSubscriptionPoolNameCollisionAcrossSubs(t *testing.T) {
	// 两个订阅有同名节点：mihomo 里名字必须唯一，必须自动消歧
	s1 := newSubServer(t, `
  - {name: "same", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)
	s2 := newSubServer(t, `
  - {name: "same", type: ss, server: b.example.com, port: 2, cipher: aes-128-gcm, password: y}`)

	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.AddMany([]SubscriptionSpec{
		{Name: "sub-a", URL: s1.URL},
		{Name: "sub-b", URL: s2.URL},
	})
	p.RefreshAll(context.Background(), true)

	nodes, _ := p.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
	seen := map[string]bool{}
	for _, n := range nodes {
		if seen[n.Name] {
			t.Fatalf("节点名重复: %s", n.Name)
		}
		seen[n.Name] = true
	}
}

// TestSubscriptionPoolNoDedupeByDefault 是本设计的关键约束：
// 同一物理节点出现在不同订阅时，默认保留为两条。
//
// 理由：两个订阅的额度、限速、到期时间各自独立，合并后无法追踪
// 实际消耗了谁的额度。
func TestSubscriptionPoolNoDedupeByDefault(t *testing.T) {
	same := `
  - {name: "N1", type: ss, server: same.example.com, port: 443, cipher: aes-128-gcm, password: pw, uuid: u1}`
	s1 := newSubServer(t, same)
	s2 := newSubServer(t, same)

	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.AddMany([]SubscriptionSpec{
		{Name: "sub-a", URL: s1.URL},
		{Name: "sub-b", URL: s2.URL},
	})
	p.RefreshAll(context.Background(), true)

	nodes, _ := p.Nodes()
	if len(nodes) != 2 {
		t.Fatalf("默认不应跨订阅去重，期望 2 条，得到 %d", len(nodes))
	}
	subs := map[string]bool{}
	for _, n := range nodes {
		subs[n.Subscription] = true
	}
	if !subs["sub-a"] || !subs["sub-b"] {
		t.Fatalf("两条节点应分属不同订阅: %v", subs)
	}
}

func TestSubscriptionPoolDedupeOptIn(t *testing.T) {
	same := `
  - {name: "N1", type: ss, server: same.example.com, port: 443, cipher: aes-128-gcm, password: pw}`
	s1 := newSubServer(t, same)
	s2 := newSubServer(t, same)

	p := NewSubscriptionPool(SubscriptionPoolOptions{DedupeAcrossSubs: true})
	_ = p.AddMany([]SubscriptionSpec{
		{Name: "sub-a", URL: s1.URL},
		{Name: "sub-b", URL: s2.URL},
	})
	p.RefreshAll(context.Background(), true)

	nodes, _ := p.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("显式开启去重后期望 1 条，得到 %d", len(nodes))
	}
}

func TestSubscriptionPoolDisable(t *testing.T) {
	s := newSubServer(t, `
  - {name: "n1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.Add(SubscriptionSpec{Name: "s", URL: s.URL})
	p.RefreshAll(context.Background(), true)

	if nodes, _ := p.Nodes(); len(nodes) != 1 {
		t.Fatalf("期望 1 个节点，得到 %d", len(nodes))
	}

	if err := p.SetEnabled("s", false); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if nodes, _ := p.Nodes(); len(nodes) != 0 {
		t.Fatalf("禁用后不应贡献节点，得到 %d", len(nodes))
	}

	if err := p.SetEnabled("s", true); err != nil {
		t.Fatalf("SetEnabled: %v", err)
	}
	if nodes, _ := p.Nodes(); len(nodes) != 1 {
		t.Fatalf("重新启用后应恢复，得到 %d", len(nodes))
	}

	if err := p.SetEnabled("nope", true); err == nil {
		t.Fatal("不存在的订阅应报错")
	}
}

func TestSubscriptionPoolIntervalScheduling(t *testing.T) {
	s := newSubServer(t, `
  - {name: "n1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)
	p := NewSubscriptionPool(SubscriptionPoolOptions{DefaultInterval: time.Hour})
	_ = p.Add(SubscriptionSpec{Name: "s", URL: s.URL})

	ok, _ := p.RefreshAll(context.Background(), false)
	if ok != 1 {
		t.Fatalf("首次应刷新（nextAt 已到期），得到 %d", ok)
	}
	// 再刷一次应跳过（未到期）
	ok, _ = p.RefreshAll(context.Background(), false)
	if ok != 0 {
		t.Fatalf("未到期不应刷新，得到 %d", ok)
	}
	// force 应强制刷新
	ok, _ = p.RefreshAll(context.Background(), true)
	if ok != 1 {
		t.Fatalf("force 应刷新，得到 %d", ok)
	}
}

func TestSubscriptionPoolTagsApplied(t *testing.T) {
	s := newSubServer(t, `
  - {name: "🇭🇰香港高速01|BGP", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.Add(SubscriptionSpec{
		Name: "s", URL: s.URL,
		Tags: []string{"airport:premium"},
	})
	p.RefreshAll(context.Background(), true)

	nodes, _ := p.Nodes()
	if len(nodes) != 1 {
		t.Fatalf("期望 1 个节点，得到 %d", len(nodes))
	}
	tags := nodes[0].Tags
	for _, want := range []string{"region:HK", "bgp", "airport:premium"} {
		if !HasTag(tags, want) {
			t.Fatalf("缺少标签 %s，实际 %v", want, tags)
		}
	}
}

func TestSubscriptionPoolMaskURL(t *testing.T) {
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.Add(SubscriptionSpec{
		Name: "s",
		URL:  "https://airport.example/api/v1/client/subscribe?token=SECRET_TOKEN_VALUE",
	})
	infos := p.Infos()
	if len(infos) != 1 {
		t.Fatalf("期望 1 个 info，得到 %d", len(infos))
	}
	if contains([]byte(infos[0].URL), []byte("SECRET_TOKEN_VALUE")) {
		t.Fatalf("URL 未打码: %s", infos[0].URL)
	}
}

func contains(hay, needle []byte) bool {
	return len(needle) > 0 && len(hay) >= len(needle) && indexBytes(hay, needle) >= 0
}

func indexBytes(hay, needle []byte) int {
	for i := 0; i+len(needle) <= len(hay); i++ {
		match := true
		for j := range needle {
			if hay[i+j] != needle[j] {
				match = false
				break
			}
		}
		if match {
			return i
		}
	}
	return -1
}

func TestSubscriptionPoolRemove(t *testing.T) {
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.Add(SubscriptionSpec{Name: "a", URL: "https://a/x"})
	_ = p.Add(SubscriptionSpec{Name: "b", URL: "https://b/x"})

	if !p.Remove("a") {
		t.Fatal("Remove 应返回 true")
	}
	if p.Remove("a") {
		t.Fatal("重复 Remove 应返回 false")
	}
	names := p.Names()
	if len(names) != 1 || names[0] != "b" {
		t.Fatalf("移除后应只剩 b: %v", names)
	}
}

// —— 配额测试 ——

func TestQuotaTrackerBasic(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.5)

	// 首个请求总能通过（total=0 时不判）
	if !q.Acquire("a") {
		t.Fatal("首个请求应放行")
	}
	if !q.Acquire("b") {
		t.Fatal("b 无配额限制，应放行")
	}
	// 此时 a:1 b:1 total:2，a 再占会变成 2/3 = 66% > 50%
	if q.Acquire("a") {
		t.Fatal("a 加一个后为 2/3，超过 50% 应拒绝")
	}
	// b 可以继续
	if !q.Acquire("b") {
		t.Fatal("b 无限制应放行")
	}
	if !q.Acquire("b") {
		t.Fatal("b 无限制应放行")
	}
	// 现在 a:1 b:3 total:4，a 加一个 = 2/5 = 40% <= 50%，允许
	if !q.Acquire("a") {
		t.Fatal("40% 在上限内应放行")
	}
}

func TestQuotaTrackerRelease(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.5)
	q.Acquire("a")
	q.Acquire("b")
	q.Acquire("b")

	snap := q.Snapshot()
	if snap["a"] <= 0 {
		t.Fatalf("快照应含 a: %v", snap)
	}

	q.Release("a")
	q.Release("b")
	q.Release("b")
	snap = q.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("全部释放后快照应为空: %v", snap)
	}
}

func TestQuotaTrackerEmptySubAlwaysAllowed(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.1)
	// inline 节点（无订阅归属）不受配额约束
	for i := 0; i < 20; i++ {
		if !q.Acquire("") {
			t.Fatalf("inline 节点不应受限（第 %d 次）", i)
		}
	}
}

func TestQuotaFilterFallsBackWhenAllBlocked(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.01)
	q.Acquire("a")
	q.Acquire("a")

	nodes := []*Node{
		{Name: "n1", Subscription: "a", Alive: true, DelayMS: 50},
	}
	got := q.filterByQuota(nodes)
	if len(got) != 1 {
		t.Fatalf("全部超配额时应退回原候选，得到 %d", len(got))
	}
}

func TestQuotaFilterDropsBlockedSub(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.1)
	// 让 a 占满
	for i := 0; i < 10; i++ {
		q.Acquire("a")
	}

	nodes := []*Node{
		{Name: "a1", Subscription: "a", Alive: true, DelayMS: 10},
		{Name: "b1", Subscription: "b", Alive: true, DelayMS: 100},
	}
	got := q.filterByQuota(nodes)
	if len(got) != 1 || got[0].Name != "b1" {
		t.Fatalf("超配额的 a 应被剔除，得到 %v", nodeNamesPtr(got))
	}
}

func nodeNamesPtr(ns []*Node) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Name)
	}
	return out
}

func TestQuotaConcurrentSafety(t *testing.T) {
	q := newQuotaTracker()
	q.SetLimit("a", 0.5)

	var wg sync.WaitGroup
	var okCount int64
	var mu sync.Mutex
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub := "a"
			if i%2 == 0 {
				sub = "b"
			}
			if q.Acquire(sub) {
				mu.Lock()
				okCount++
				mu.Unlock()
				q.Release(sub)
			}
		}(i)
	}
	wg.Wait()

	if okCount == 0 {
		t.Fatal("并发下应至少有一些请求通过")
	}
}

func TestSubscriptionPoolQuotaApplied(t *testing.T) {
	s1 := newSubServer(t, `
  - {name: "n1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`)
	p := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = p.Add(SubscriptionSpec{Name: "s", URL: s1.URL, Quota: 0.3})
	p.RefreshAll(context.Background(), true)

	if got := p.QuotaOf("s"); got != 0.3 {
		t.Fatalf("QuotaOf 应返回 0.3，得到 %v", got)
	}
	if got := p.QuotaOf("nope"); got != 0 {
		t.Fatalf("不存在的订阅应返回 0，得到 %v", got)
	}
}

// TestSubscriptionPoolHealthCountsPerSub 回归：跨订阅健康统计不能互相污染。
//
// 踩过的 bug：attachMeta 用 rt.nodes 里的原始 Name 建索引，
// 而 Nodes() 会把重名节点改名成 name@sub。结果第二个订阅的节点
// 全部命中第一个订阅的同名条目，a2 显示 80 个健康节点（实际 40）。
func TestSubscriptionPoolHealthCountsPerSub(t *testing.T) {
	same := `
  - {name: "N1", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}
  - {name: "N2", type: ss, server: b.example.com, port: 2, cipher: aes-128-gcm, password: y}`
	s1 := newSubServer(t, same)
	s2 := newSubServer(t, same)

	sp := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = sp.AddMany([]SubscriptionSpec{
		{Name: "a1", URL: s1.URL},
		{Name: "a2", URL: s2.URL},
	})
	sp.RefreshAll(context.Background(), true)

	nodes, err := sp.Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("期望 4 个节点，得到 %d", len(nodes))
	}

	bySub := map[string]int{}
	for _, n := range nodes {
		bySub[n.Subscription]++
	}
	if bySub["a1"] != 2 || bySub["a2"] != 2 {
		t.Fatalf("每个订阅应各 2 个节点，得到 %v", bySub)
	}
}

// TestAttachMetaMapsRenameCorrectly 验证改名后归属仍然正确。
func TestAttachMetaMapsRenameCorrectly(t *testing.T) {
	same := `
  - {name: "dup", type: ss, server: a.example.com, port: 1, cipher: aes-128-gcm, password: x}`
	s1 := newSubServer(t, same)
	s2 := newSubServer(t, same)

	sp := NewSubscriptionPool(SubscriptionPoolOptions{})
	_ = sp.AddMany([]SubscriptionSpec{
		{Name: "a1", URL: s1.URL},
		{Name: "a2", URL: s2.URL},
	})
	sp.RefreshAll(context.Background(), true)

	merged, _ := sp.Nodes()
	meta := map[string]string{}
	for _, n := range merged {
		meta[n.Name] = n.Subscription
	}
	if len(meta) != 2 {
		t.Fatalf("期望 2 个唯一条目，得到 %d: %v", len(meta), meta)
	}
	// 一条原名，一条带 @a2
	if meta["dup"] != "a1" {
		t.Fatalf("原名的归属应为 a1，得到 %q", meta["dup"])
	}
	if meta["dup@a2"] != "a2" {
		t.Fatalf("改名后的归属应为 a2，得到 %q", meta["dup@a2"])
	}
}
