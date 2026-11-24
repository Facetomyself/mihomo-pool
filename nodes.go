package mihomopool

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 节点视图与并发测速。
//
// 节点来源有两条：
//   - inline 节点：Start/Reload 时由配置直接写死，端口在 Bindings 里预先分配
//   - provider 节点：订阅由内核自拉，节点名运行时才知道，没有独立端口
//
// 两类节点都能参与"按延迟/权重分发"，但只有 inline 节点能提供
// 独立端口的强隔离。provider 节点只能经共享端口 + selector 使用，
// 高并发下不要依赖它。

// Node 是模块视角下的一个可用出站。
type Node struct {
	// Name 是 mihomo 里的节点名。
	Name string
	// Type 是节点类型（ss/vmess/trojan/...）。
	Type string
	// Group 是业务分组（inline 节点由 NodeSpec.Group 给出，provider 节点为 provider 名）。
	Group string
	// Provider 是来源 provider，inline 节点为空。
	Provider string
	// ProxyURL 是该节点专属的本地代理地址；provider 节点为空表示无独立端口。
	ProxyURL string
	// Port 是独立本地端口，0 表示没有。
	Port int
	// DelayMS 是最近一次测速延迟，-1 表示未测速。
	DelayMS int64
	// Alive 是内核认为的存活状态。
	Alive bool

	// Subscription 是该节点的来源订阅名；inline 节点为空。
	//
	// 这是订阅池的核心字段：配额、权重、统计、去重判定全部按它归集。
	// 同一物理节点若来自不同订阅，会保留为两条独立记录——因为
	// 两个订阅的额度、限速、到期时间是各自独立的，合并会丢失归属。
	Subscription string

	// Tags 是从节点名/类型提取的细粒度标签（地区、倍率、流媒体等）。
	Tags []string

	// —— 运行期统计，由分发器维护 ——
	inflight  atomic.Int64
	okCount   atomic.Int64
	failCount atomic.Int64
	lastFail  atomic.Int64 // unix nano
}

// Inflight 返回当前在飞请求数。
func (n *Node) Inflight() int64 { return n.inflight.Load() }

// Score 返回调度用的综合分，越小越优先。
//
// 打分规则（可解释、无隐藏状态）：
//   - 失败过的节点按失败次数线性抬高基础分，避免反复撞墙
//   - 最近一次失败在 30s 内，额外罚分（冷却）
//   - 延迟 >0 时直接参与比较；未测速按 500ms 计入
//   - 在飞数 × 20ms 作为负载惩罚，天然实现"最少在飞优先"
func (n *Node) Score() int64 {
	delay := n.DelayMS
	if delay <= 0 {
		delay = 500
	}
	base := delay
	fails := n.failCount.Load()
	if fails > 0 {
		// 惩罚线性累加且倍数较高：连续失败 3 次即越过健康阈值，
		// 避免"屡败仍在候选里排前面"。
		base += fails * 800
		if lf := n.lastFail.Load(); lf > 0 {
			if time.Since(time.Unix(0, lf)) < 30*time.Second {
				base += 600
			}
		}
	}
	base += n.inflight.Load() * 20
	return base
}

// Healthy 判断节点当前是否可用。
func (n *Node) Healthy() bool {
	if !n.Alive {
		return false
	}
	return n.Score() < 3000
}

// NodeFilter 是节点筛选条件。
type NodeFilter struct {
	// Groups 非空时只保留这些分组。
	Groups []string
	// Types 非空时只保留这些类型。
	Types []string
	// RequirePort 为 true 时只保留有独立端口的节点。
	RequirePort bool
	// HealthyOnly 为 true 时丢弃不健康节点。
	HealthyOnly bool
	// NamePrefix 按名字前缀筛选。
	NamePrefix string
}

// Match 判断节点是否满足筛选条件。
func (f NodeFilter) Match(n *Node) bool {
	if f.RequirePort && n.Port == 0 {
		return false
	}
	if f.HealthyOnly && !n.Healthy() {
		return false
	}
	if f.NamePrefix != "" && !strings.HasPrefix(n.Name, f.NamePrefix) {
		return false
	}
	// 空串/纯空白的筛选值视为未设置，避免调用方传 []string{""} 时
	// 把所有节点都筛掉（这是极易踩的静默坑）。
	if g := nonEmptyList(f.Groups); len(g) > 0 && !containsFold(g, n.Group) {
		return false
	}
	if t := nonEmptyList(f.Types); len(t) > 0 && !containsFold(t, n.Type) {
		return false
	}
	return true
}

// NodeSet 是线程安全的节点集合。
type NodeSet struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	order []string
}

// newNodeSet 构造空集合。
func newNodeSet() *NodeSet {
	return &NodeSet{nodes: make(map[string]*Node)}
}

// Replace 用新列表整体替换（保留同名节点的运行期统计）。
//
// 同名节点是换掉指针而不是原地改字段：读者手里的快照永远自洽，
// 不需要与写者共用锁。统计值（在飞/成败计数）显式拷到新节点上。
func (s *NodeSet) Replace(list []*Node) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[string]*Node, len(list))
	order := make([]string, 0, len(list))
	for _, n := range list {
		if n == nil || n.Name == "" {
			continue
		}
		if old, ok := s.nodes[n.Name]; ok {
			cp := cloneNode(n)
			cp.inflight.Store(old.inflight.Load())
			cp.okCount.Store(old.okCount.Load())
			cp.failCount.Store(old.failCount.Load())
			cp.lastFail.Store(old.lastFail.Load())
			if cp.DelayMS <= 0 {
				cp.DelayMS = old.DelayMS
			}
			next[n.Name] = cp
		} else {
			next[n.Name] = n
		}
		order = append(order, n.Name)
	}
	s.nodes = next
	s.order = order
}

// update 对指定节点做写时复制式修改。
//
// 节点字段不原地改：Filter/Pick 等读路径不加锁，
// 原地改 Alive/DelayMS/Tags 会与读者产生数据竞争。
func (s *NodeSet) update(name string, mutate func(*Node)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.nodes[name]
	if !ok {
		return
	}
	cp := cloneNode(old)
	mutate(cp)
	s.nodes[name] = cp
}

// cloneNode 复制节点，运行期统计一并带走。
func cloneNode(n *Node) *Node {
	cp := &Node{
		Name:         n.Name,
		Type:         n.Type,
		Group:        n.Group,
		Provider:     n.Provider,
		ProxyURL:     n.ProxyURL,
		Port:         n.Port,
		DelayMS:      n.DelayMS,
		Alive:        n.Alive,
		Subscription: n.Subscription,
		Tags:         append([]string(nil), n.Tags...),
	}
	cp.inflight.Store(n.inflight.Load())
	cp.okCount.Store(n.okCount.Load())
	cp.failCount.Store(n.failCount.Load())
	cp.lastFail.Store(n.lastFail.Load())
	return cp
}

// All 返回全部节点的副本切片（按插入顺序）。
func (s *NodeSet) All() []*Node {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Node, 0, len(s.order))
	for _, name := range s.order {
		if n, ok := s.nodes[name]; ok {
			out = append(out, n)
		}
	}
	return out
}

// Get 按名字取节点。
func (s *NodeSet) Get(name string) (*Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[name]
	return n, ok
}

// Len 返回节点数。
func (s *NodeSet) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.nodes)
}

// Filter 按条件筛选并按 Score 升序返回。
func (s *NodeSet) Filter(f NodeFilter) []*Node {
	all := s.All()
	out := make([]*Node, 0, len(all))
	for _, n := range all {
		if f.Match(n) {
			out = append(out, n)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		si, sj := out[i].Score(), out[j].Score()
		if si == sj {
			return out[i].Name < out[j].Name
		}
		return si < sj
	})
	return out
}

// DelayResult 是一次并发测速的结果。
type DelayResult struct {
	Node    string
	DelayMS int64
	OK      bool
	Error   string
}

// DelayAll 对节点并发测速。
//
// concurrency 控制同时进行的测速数（内核侧也有上限，这里默认 16）。
// 结果会写回节点的 DelayMS / Alive。
func (s *NodeSet) DelayAll(ctx context.Context, ctl *Control, names []string, testURL string, timeout time.Duration, concurrency int) []DelayResult {
	if concurrency <= 0 {
		concurrency = 16
	}
	targets := names
	if len(targets) == 0 {
		for _, n := range s.All() {
			targets = append(targets, n.Name)
		}
	}
	if len(targets) == 0 {
		return nil
	}

	results := make([]DelayResult, len(targets))
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i, name := range targets {
		wg.Add(1)
		go func(idx int, nm string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] = DelayResult{Node: nm, DelayMS: -1, Error: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()

			d, err := ctl.Delay(ctx, nm, testURL, timeout)
			if err != nil {
				results[idx] = DelayResult{Node: nm, DelayMS: -1, Error: err.Error()}
				s.update(nm, func(x *Node) { x.Alive = false })
				return
			}
			results[idx] = DelayResult{Node: nm, DelayMS: d, OK: true}
			s.update(nm, func(x *Node) {
				x.DelayMS = d
				x.Alive = true
			})
		}(i, name)
	}
	wg.Wait()
	return results
}

// Refresh 从控制面重新拉取节点列表并合并进集合。
//
// 已有独立端口映射通过 bindings 保留；provider 节点没有端口。
func (s *NodeSet) Refresh(ctx context.Context, ctl *Control, bindings []listenerBinding) error {
	items, err := ctl.Nodes(ctx)
	if err != nil {
		return err
	}
	portOf := make(map[string]listenerBinding, len(bindings))
	for _, b := range bindings {
		portOf[b.Node] = b
	}

	list := make([]*Node, 0, len(items))
	for _, it := range items {
		n := &Node{
			Name:     it.Name,
			Type:     it.Type,
			Provider: it.ProviderName,
			Group:    it.ProviderName,
			Alive:    it.Alive,
			DelayMS:  it.Delay(),
		}
		if b, ok := portOf[it.Name]; ok {
			n.Port = b.Port
			n.ProxyURL = b.ProxyURL
		}
		list = append(list, n)
	}
	s.Replace(list)
	return nil
}

func containsFold(list []string, v string) bool {
	for _, x := range list {
		if strings.EqualFold(strings.TrimSpace(x), strings.TrimSpace(v)) {
			return true
		}
	}
	return false
}

// nonEmptyList 去掉空串与纯空白项。
func nonEmptyList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		if strings.TrimSpace(v) != "" {
			out = append(out, v)
		}
	}
	return out
}
