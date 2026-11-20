package mihomopool

import (
	"testing"
	"time"
)

func mkNode(name string, delay int64, alive bool) *Node {
	return &Node{Name: name, Type: "ss", DelayMS: delay, Alive: alive}
}

func TestDispatcherRejectsUnknownStrategy(t *testing.T) {
	if _, err := NewDispatcher("nope"); err == nil {
		t.Fatal("期望未知策略报错")
	}
}

func TestDispatcherLeastInflightRespectsInflight(t *testing.T) {
	d, _ := NewDispatcher(StrategyLeastInflight)
	a := mkNode("a", 100, true)
	b := mkNode("b", 100, true)
	// a 有 3 个在飞，同一延迟下应选 b
	a.inflight.Store(3)
	got, err := d.Pick([]*Node{a, b}, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Name != "b" {
		t.Fatalf("期望选 b，得到 %s", got.Name)
	}
}

func TestDispatcherLowestLatency(t *testing.T) {
	d, _ := NewDispatcher(StrategyLowestLatency)
	nodes := []*Node{mkNode("slow", 300, true), mkNode("fast", 50, true), mkNode("mid", 120, true)}
	got, err := d.Pick(nodes, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Name != "fast" {
		t.Fatalf("期望 fast，得到 %s", got.Name)
	}
}

func TestDispatcherRoundRobinCoversAll(t *testing.T) {
	d, _ := NewDispatcher(StrategyRoundRobin)
	nodes := []*Node{mkNode("a", 100, true), mkNode("b", 100, true), mkNode("c", 100, true)}
	seen := map[string]int{}
	for i := 0; i < 9; i++ {
		n, err := d.Pick(nodes, "")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		seen[n.Name]++
	}
	if len(seen) != 3 {
		t.Fatalf("轮询没覆盖全部节点: %v", seen)
	}
	for name, c := range seen {
		if c != 3 {
			t.Fatalf("节点 %s 命中 %d 次，期望 3", name, c)
		}
	}
}

func TestDispatcherHashStable(t *testing.T) {
	d, _ := NewDispatcher(StrategyHash)
	nodes := []*Node{mkNode("a", 100, true), mkNode("b", 100, true), mkNode("c", 100, true)}

	first, _ := d.Pick(nodes, "https://target.example.com/api/1")
	for i := 0; i < 20; i++ {
		got, err := d.Pick(nodes, "https://target.example.com/api/1")
		if err != nil {
			t.Fatalf("Pick: %v", err)
		}
		if got.Name != first.Name {
			t.Fatalf("同一 key 落到不同节点: %s vs %s", first.Name, got.Name)
		}
	}
	// 不同 key 应能分散
	distinct := map[string]bool{first.Name: true}
	for i := 0; i < 30; i++ {
		got, _ := d.Pick(nodes, "key-"+string(rune('a'+i)))
		distinct[got.Name] = true
	}
	if len(distinct) < 2 {
		t.Fatalf("不同 key 未分散: %v", distinct)
	}
}

func TestDispatcherSkipsUnhealthy(t *testing.T) {
	d, _ := NewDispatcher(StrategyLeastInflight)
	dead := mkNode("dead", 100, false) // Alive=false
	good := mkNode("good", 900, true)
	got, err := d.Pick([]*Node{dead, good}, "")
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if got.Name != "good" {
		t.Fatalf("期望 good，得到 %s", got.Name)
	}
}

func TestDispatcherAllUnhealthyErrors(t *testing.T) {
	d, _ := NewDispatcher(StrategyLeastInflight)
	if _, err := d.Pick([]*Node{mkNode("x", 100, false)}, ""); err == nil {
		t.Fatal("期望无可用节点报错")
	}
	if _, err := d.Pick(nil, ""); err == nil {
		t.Fatal("空候选应报错")
	}
}

func TestNodeScorePenalizesFailure(t *testing.T) {
	n := mkNode("n", 100, true)
	base := n.Score()
	n.failCount.Store(2)
	n.lastFail.Store(nowNano())
	if got := n.Score(); got <= base {
		t.Fatalf("失败后分数应上升: %d → %d", base, got)
	}
}

func TestNodeFilterMatch(t *testing.T) {
	n := &Node{Name: "hk-1", Type: "ss", Group: "hk", Port: 18000, Alive: true, DelayMS: 80}

	if !(NodeFilter{}).Match(n) {
		t.Fatal("空筛选应全通过")
	}
	if !(NodeFilter{RequirePort: true}).Match(n) {
		t.Fatal("有端口应通过 RequirePort")
	}
	if (NodeFilter{Groups: []string{"us"}}).Match(n) {
		t.Fatal("分组不匹配应被筛掉")
	}
	if !(NodeFilter{Groups: []string{"HK"}}).Match(n) {
		t.Fatal("分组应大小写不敏感")
	}
	if !(NodeFilter{NamePrefix: "hk"}).Match(n) {
		t.Fatal("前缀匹配失败")
	}
	noPort := &Node{Name: "p", Type: "ss", Alive: true, DelayMS: 80}
	if (NodeFilter{RequirePort: true}).Match(noPort) {
		t.Fatal("无端口应被 RequirePort 筛掉")
	}
}

func TestNodeSetReplaceKeepsStats(t *testing.T) {
	s := newNodeSet()
	n1 := mkNode("a", 100, true)
	n1.okCount.Store(7)
	n1.Port = 18000
	s.Replace([]*Node{n1})

	n2 := mkNode("a", 200, true)
	n2.Port = 18000
	s.Replace([]*Node{n2})

	got, ok := s.Get("a")
	if !ok {
		t.Fatal("节点丢失")
	}
	if got.okCount.Load() != 7 {
		t.Fatalf("统计未保留: %d", got.okCount.Load())
	}
	if got.DelayMS != 200 {
		t.Fatalf("延迟未更新: %d", got.DelayMS)
	}
}

func TestNodeSetFilterSortsByScore(t *testing.T) {
	s := newNodeSet()
	slow := mkNode("slow", 400, true)
	fast := mkNode("fast", 50, true)
	s.Replace([]*Node{slow, fast})

	got := s.Filter(NodeFilter{})
	if len(got) != 2 || got[0].Name != "fast" {
		t.Fatalf("排序不对: %v", names(got))
	}
}

func TestNodeSetLenAndAll(t *testing.T) {
	s := newNodeSet()
	s.Replace([]*Node{mkNode("a", 10, true), mkNode("b", 20, true), nil, {}})
	if s.Len() != 2 {
		t.Fatalf("期望 2 个有效节点，得到 %d", s.Len())
	}
}

func names(ns []*Node) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Name)
	}
	return out
}

func nowNano() int64 { return time.Now().UnixNano() }
