package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 订阅池：多个订阅的统一编排、健康熔断、权重配额与细粒度调度。
//
// 与"单订阅抓一次"或"交给内核对拉"的区别：
//   - 订阅级隔离：一个订阅挂了不影响其他，节点池里其他订阅的节点照常服务
//   - 订阅级健康：按成功率/延迟/连续失败算出权重，熔断的订阅暂时不派活
//   - 配额：可限制单订阅最多占用多少比例的在飞请求，避免一个订阅被压垮
//   - 细粒度标签：节点带 region/rate/stream 等标签，可按标签筛选
//
// 关于「同一物理节点出现在多个订阅」：
//
// 默认**不合并**。两个订阅的额度、限速、到期时间是各自独立的，
// 合并后就无法知道实际消耗了谁的额度。因此每个订阅的节点各自成条，
// 用 Node.Subscription 标明归属；调度、配额、统计都按归属归集。
// 需要合并时显式开 DedupeAcrossSubs，并清楚放弃归属追踪。

// SubscriptionState 是订阅的运行状态。
type SubscriptionState string

const (
	// SubHealthy 正常，参与调度。
	SubHealthy SubscriptionState = "healthy"
	// SubDegraded 有失败但未熔断，降权重继续用。
	SubDegraded SubscriptionState = "degraded"
	// SubCircuitOpen 连续失败达到阈值，暂时不派活。
	SubCircuitOpen SubscriptionState = "circuit-open"
	// SubDisabled 被手动禁用。
	SubDisabled SubscriptionState = "disabled"
)

// SubscriptionSpec 是订阅池里一个订阅的配置。
type SubscriptionSpec struct {
	// Name 是唯一标识，也是 Node.Subscription 的值。
	Name string
	// URL 是订阅地址。
	URL string
	// Interval 是刷新间隔，0 用池默认值。
	Interval time.Duration
	// Priority 是静态优先级，越大越优先（0 = 不额外加权）。
	Priority int
	// Weight 是静态权重倍数，与动态健康分相乘（0 = 当作 1）。
	Weight float64
	// Quota 是允许占用的最大在飞比例（0~1，0 = 不限）。
	// 例如 0.4 表示这一个订阅最多占 40% 的在飞请求。
	Quota float64
	// Tags 是给该订阅下所有节点附加的标签，便于按订阅标签筛选。
	Tags []string
	// Parse 是该订阅的解析清洗选项。
	Parse ParseOptions
	// Disabled 为 true 时不参与调度。
	Disabled bool
	// Header 是该订阅的额外请求头（鉴权等）。
	Header map[string]string
}

// SubscriptionHealth 是一个订阅的运行统计。
type SubscriptionHealth struct {
	// RefreshOK / RefreshFail 是刷新成功/失败次数。
	RefreshOK   int64
	RefreshFail int64
	// ConsecutiveFail 是连续刷新失败次数，熔断判定用。
	ConsecutiveFail int64
	// LastRefresh 是最近一次刷新时间。
	LastRefresh time.Time
	// LastError 是最近一次错误。
	LastError string
}

// SubscriptionInfo 是给外部看的订阅快照。
type SubscriptionInfo struct {
	Name        string
	URL         string // 打码后的地址
	State       SubscriptionState
	Nodes       int
	Healthy     int
	Priority    int
	Weight      float64
	Quota       float64
	EffWeight   float64 // 动态权重（健康分 × 静态权重）
	AvgDelayMS  int64
	RefreshOK   int64
	RefreshFail int64
	ConsecFail  int64
	LastRefresh time.Time
	LastError   string
	NextRefresh time.Time
}

// subRuntime 是订阅的运行时状态。
type subRuntime struct {
	spec SubscriptionSpec

	mu       sync.Mutex
	nodes    []NodeSpec
	state    SubscriptionState
	health   SubscriptionHealth
	nextAt   time.Time
	avgDelay int64

	// 在飞计数由 pool 的配额判定使用
	inflight atomic.Int64
}

// SubscriptionPool 管理一组订阅。
type SubscriptionPool struct {
	mu    sync.RWMutex
	subs  map[string]*subRuntime
	order []string

	opts SubscriptionPoolOptions

	// healthBySub 是最近的运行态健康数快照，由 SyncHealth 更新。
	healthBySub map[string]int
}

// SyncHealth 从运行中的 Pool 同步各订阅的健康节点数。
//
// 订阅池自身只掌握订阅维度；节点存活要问内核，
// 所以这里从 Pool 的 NodeSet 按 Subscription 归集。
func (p *SubscriptionPool) SyncHealth(pool *Pool) {
	if pool == nil || pool.nodes == nil {
		return
	}
	counts := map[string]int{}
	for _, n := range pool.nodes.All() {
		if n.Subscription != "" && n.Healthy() {
			counts[n.Subscription]++
		}
	}
	p.mu.Lock()
	p.healthBySub = counts
	p.mu.Unlock()
}

// SubscriptionPoolOptions 控制订阅池行为。
type SubscriptionPoolOptions struct {
	// DefaultInterval 是未指定间隔时的刷新周期，默认 1 小时。
	DefaultInterval time.Duration
	// Fetch 是拉取配置（UA、超时、字节上限）。
	Fetch FetchOptions
	// MaxConsecutiveFail 是熔断阈值：连续失败这么多次后暂停派活。默认 3。
	MaxConsecutiveFail int
	// CircuitHalfOpen 是熔断后多久尝试恢复（半开）。默认 5 分钟。
	CircuitHalfOpen time.Duration
	// DedupeAcrossSubs 为 true 时跨订阅按连接指纹去重（保留权重最高的）。
	// 默认 false：不同订阅的额度独立，合并会丢失归属。
	DedupeAcrossSubs bool
	// Clean 是全局解析清洗选项，会与该订阅自己的 Parse 合并。
	Clean ParseOptions
}

// normalize 补默认值。
func (o *SubscriptionPoolOptions) normalize() {
	if o.DefaultInterval <= 0 {
		o.DefaultInterval = time.Hour
	}
	if o.MaxConsecutiveFail <= 0 {
		o.MaxConsecutiveFail = 3
	}
	if o.CircuitHalfOpen <= 0 {
		o.CircuitHalfOpen = 5 * time.Minute
	}
}

// NewSubscriptionPool 构造订阅池。
func NewSubscriptionPool(opts SubscriptionPoolOptions) *SubscriptionPool {
	opts.normalize()
	return &SubscriptionPool{
		subs: make(map[string]*subRuntime),
		opts: opts,
	}
}

// Add 注册一个订阅（不立即拉取）。
func (p *SubscriptionPool) Add(spec SubscriptionSpec) error {
	name := strings.TrimSpace(spec.Name)
	if name == "" {
		return errors.New("mihomo-pool: 订阅必须有 Name")
	}
	if strings.TrimSpace(spec.URL) == "" {
		return fmt.Errorf("mihomo-pool: 订阅 %s 缺少 URL", name)
	}
	if spec.Interval <= 0 {
		spec.Interval = p.opts.DefaultInterval
	}
	if spec.Weight <= 0 {
		spec.Weight = 1
	}
	if spec.Quota < 0 || spec.Quota > 1 {
		return fmt.Errorf("mihomo-pool: 订阅 %s 的 Quota 必须在 0~1 之间", name)
	}
	spec.Name = name

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, dup := p.subs[name]; dup {
		return fmt.Errorf("mihomo-pool: 订阅名重复: %s", name)
	}

	rt := &subRuntime{spec: spec, state: SubHealthy, nextAt: time.Now()}
	if spec.Disabled {
		rt.state = SubDisabled
	}
	p.subs[name] = rt
	p.order = append(p.order, name)
	return nil
}

// AddMany 批量注册。
func (p *SubscriptionPool) AddMany(specs []SubscriptionSpec) error {
	for _, s := range specs {
		if err := p.Add(s); err != nil {
			return err
		}
	}
	return nil
}

// Remove 移除订阅及其节点。
func (p *SubscriptionPool) Remove(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.subs[name]; !ok {
		return false
	}
	delete(p.subs, name)
	next := p.order[:0]
	for _, n := range p.order {
		if n != name {
			next = append(next, n)
		}
	}
	p.order = next
	return true
}

// Names 返回订阅名的稳定顺序。
func (p *SubscriptionPool) Names() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, len(p.order))
	copy(out, p.order)
	return out
}

// RefreshAll 并发拉取所有到期（或 force）的订阅。
//
// 每个订阅独立成败：一个失败只影响它自己的节点与权重，
// 不影响其他订阅的节点继续服务。
func (p *SubscriptionPool) RefreshAll(ctx context.Context, force bool) (updated int, failed int) {
	p.mu.RLock()
	names := make([]string, len(p.order))
	copy(names, p.order)
	p.mu.RUnlock()

	type res struct {
		name string
		n    int
		err  error
	}
	ch := make(chan res, len(names))
	var wg sync.WaitGroup

	for _, name := range names {
		rt := p.get(name)
		if rt == nil {
			continue
		}
		if rt.spec.Disabled {
			continue
		}
		if !force && !p.due(rt) {
			continue
		}
		wg.Add(1)
		go func(rt *subRuntime) {
			defer wg.Done()
			n, err := p.refreshOne(ctx, rt)
			ch <- res{name: rt.spec.Name, n: n, err: err}
		}(rt)
	}
	wg.Wait()
	close(ch)

	for r := range ch {
		if r.err != nil {
			failed++
		} else {
			updated++
		}
	}
	return updated, failed
}

// due 判断订阅是否到期需要刷新。
func (p *SubscriptionPool) due(rt *subRuntime) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return !time.Now().Before(rt.nextAt)
}

// refreshOne 拉取单个订阅。
func (p *SubscriptionPool) refreshOne(ctx context.Context, rt *subRuntime) (int, error) {
	fo := p.opts.Fetch
	fo.Header = mergeHeader(fo.Header, rt.spec.Header)

	// 该订阅自己的清洗选项优先，未指定时用全局
	po := rt.spec.Parse
	if po == (ParseOptions{}) {
		po = p.opts.Clean
	}

	nodes, err := FetchSubscriptionWithParse(ctx, rt.spec.URL, fo, po)

	rt.mu.Lock()
	defer rt.mu.Unlock()

	now := time.Now()
	rt.health.LastRefresh = now
	rt.nextAt = now.Add(rt.spec.Interval)

	if err != nil {
		rt.health.RefreshFail++
		rt.health.ConsecutiveFail++
		rt.health.LastError = err.Error()

		// 熔断：连续失败达阈值就暂停派活，但**保留已有节点**，
		// 拉取失败不等于已有节点立即失效（可能只是订阅服务抖了）
		if rt.health.ConsecutiveFail >= int64(p.opts.MaxConsecutiveFail) {
			rt.state = SubCircuitOpen
			rt.nextAt = now.Add(p.opts.CircuitHalfOpen)
		} else {
			rt.state = SubDegraded
		}
		return 0, err
	}

	rt.health.RefreshOK++
	rt.health.ConsecutiveFail = 0
	rt.health.LastError = ""
	if rt.state != SubDisabled {
		rt.state = SubHealthy
	}

	// 给每个节点打上归属与标签
	for i := range nodes {
		n := &nodes[i]
		n.Subscription = rt.spec.Name
		n.Type = strings.ToLower(n.Type)
		n.Tags = ExtractTags(n.Name, n.Type)
		// 订阅级标签并入
		for _, t := range rt.spec.Tags {
			if !HasTag(n.Tags, t) {
				n.Tags = append(n.Tags, t)
			}
		}
	}

	rt.nodes = nodes
	return len(nodes), nil
}

// Nodes 把池内所有订阅的节点汇总成统一的 NodeSpec 列表。
//
// 返回的节点已带 Subscription 与 Tags（在配置生成后会随节点落到 Node 上）。
func (p *SubscriptionPool) Nodes() ([]NodeSpec, error) {
	p.mu.RLock()
	names := make([]string, len(p.order))
	copy(names, p.order)
	p.mu.RUnlock()

	var out []NodeSpec
	seenName := map[string]int{}
	seenFP := map[string]bool{}

	for _, name := range names {
		rt := p.get(name)
		if rt == nil || rt.spec.Disabled {
			continue
		}
		rt.mu.Lock()
		state := rt.state
		nodes := rt.nodes
		rt.mu.Unlock()

		// 熔断的订阅不贡献节点，避免把请求派给失效出口
		if state == SubCircuitOpen {
			continue
		}

		for i := range nodes {
			nn := nodes[i]

			// 节点名跨订阅可能重复：mihomo 里名字必须唯一
			if cnt, dup := seenName[nn.Name]; dup {
				seenName[nn.Name] = cnt + 1
				nn.Name = fmt.Sprintf("%s@%s", nn.Name, name)
				if nn.Raw != nil {
					nn.Raw = cloneRaw(nn.Raw)
					nn.Raw["name"] = nn.Name
				}
			} else {
				seenName[nn.Name] = 1
			}

			if p.opts.DedupeAcrossSubs {
				fp := connFingerprint(nn.Raw)
				if fp != "" {
					if seenFP[fp] {
						continue
					}
					seenFP[fp] = true
				}
			}
			out = append(out, nn)
		}
	}
	return out, nil
}

// ApplyTo 把池内节点应用到 Pool：生成配置、起内核、注入订阅标签。
//
// 这是订阅池与内核的接合点。调用后 Nodes() 里的标记会进到运行态节点。
func (p *SubscriptionPool) ApplyTo(ctx context.Context, pool *Pool) error {
	nodes, err := p.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("mihomo-pool: 订阅池没有可用节点")
	}
	if err := pool.Start(ctx, nodes, nil); err != nil {
		return err
	}
	p.attachMeta(pool)
	p.applyQuotas(pool)
	return nil
}

// applyQuotas 把各订阅的 Quota 下发到执行器。
func (p *SubscriptionPool) applyQuotas(pool *Pool) {
	for _, name := range p.Names() {
		rt := p.get(name)
		if rt == nil {
			continue
		}
		pool.SetQuota(name, rt.spec.Quota)
	}
}

// ReloadTo 用池内最新节点重载运行中的内核。
func (p *SubscriptionPool) ReloadTo(ctx context.Context, pool *Pool) error {
	nodes, err := p.Nodes()
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		return errors.New("mihomo-pool: 订阅池没有可用节点")
	}
	if err := pool.Reload(ctx, nodes, nil); err != nil {
		return err
	}
	p.attachMeta(pool)
	p.applyQuotas(pool)
	return nil
}

// attachMeta 把订阅归属与标签写到 Pool 的运行时节点上。
//
// 内核同步回来的 Node 只知道名字，不知道它来自哪个订阅，
// 所以这里按名字回填 Subscription / Tags。
func (p *SubscriptionPool) attachMeta(pool *Pool) {
	if pool == nil || pool.nodes == nil {
		return
	}
	// 必须用 Nodes() 的输出来建映射：它做了跨订阅重名消歧
	// （第二个订阅的同名节点会变成 name@sub）。
	// 直接用 rt.nodes 里的原始 Name 建索引会让 a2 的节点全部命中 a1 的
	// 同名条目，导致两个订阅的健康统计互相污染（实测 a2 显示 80 个健康
	// 节点而它只有 40 个）。
	merged, err := p.Nodes()
	if err != nil {
		return
	}
	meta := make(map[string]*NodeSpec, len(merged))
	for i := range merged {
		meta[merged[i].Name] = &merged[i]
	}

	for _, n := range pool.nodes.All() {
		pool.nodes.update(n.Name, func(x *Node) {
			if src, ok := meta[x.Name]; ok {
				x.Subscription = src.Subscription
				x.Tags = src.Tags
			}
			// 兜底：即使匹配不上，也按名字推断标签
			if len(x.Tags) == 0 {
				x.Tags = ExtractTags(x.Name, x.Type)
			}
		})
	}
}

// Infos 返回所有订阅的快照。
func (p *SubscriptionPool) Infos() []SubscriptionInfo {
	p.mu.RLock()
	names := make([]string, len(p.order))
	copy(names, p.order)
	p.mu.RUnlock()

	out := make([]SubscriptionInfo, 0, len(names))
	for _, name := range names {
		rt := p.get(name)
		if rt == nil {
			continue
		}
		rt.mu.Lock()
		info := SubscriptionInfo{
			Name:        rt.spec.Name,
			URL:         maskURL(rt.spec.URL),
			State:       rt.state,
			Nodes:       len(rt.nodes),
			Priority:    rt.spec.Priority,
			Weight:      rt.spec.Weight,
			Quota:       rt.spec.Quota,
			RefreshOK:   rt.health.RefreshOK,
			RefreshFail: rt.health.RefreshFail,
			ConsecFail:  rt.health.ConsecutiveFail,
			LastRefresh: rt.health.LastRefresh,
			LastError:   rt.health.LastError,
			NextRefresh: rt.nextAt,
			AvgDelayMS:  rt.avgDelay,
		}
		// 健康数取自运行态快照（NodeSpec 不含存活信息）。
		// 没绑定 Pool 时退化为"节点数"，避免显示 0 误导。
		info.Healthy = len(rt.nodes)
		if p.healthBySub != nil {
			if h, ok := p.healthBySub[name]; ok {
				info.Healthy = h
			}
		}
		info.EffWeight = p.effWeight(rt)
		rt.mu.Unlock()
		out = append(out, info)
	}
	return out
}

// effWeight 计算订阅的动态权重。
//
// 组成：
//   - 静态：spec.Weight × (1 + Priority/10)
//   - 健康：成功率越高越大；连续失败按次降权
//   - 延迟：平均延迟越低越大
//
// 熔断 / 禁用的订阅权重为 0。
func (p *SubscriptionPool) effWeight(rt *subRuntime) float64 {
	if rt.state == SubCircuitOpen || rt.state == SubDisabled {
		return 0
	}
	w := rt.spec.Weight
	if rt.spec.Priority > 0 {
		w *= 1 + float64(rt.spec.Priority)/10
	}

	total := rt.health.RefreshOK + rt.health.RefreshFail
	if total > 0 {
		rate := float64(rt.health.RefreshOK) / float64(total)
		// 成功率映射到 0.2~1.0，避免完全归零后无法恢复
		w *= 0.2 + 0.8*rate
	}
	if rt.health.ConsecutiveFail > 0 {
		w /= 1 + float64(rt.health.ConsecutiveFail)
	}
	if rt.avgDelay > 0 {
		w *= 1000 / float64(rt.avgDelay+1000)
	}
	if w <= 0 {
		w = 0.01
	}
	return w
}

// get 按名字取运行时。
func (p *SubscriptionPool) get(name string) *subRuntime {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.subs[name]
}

// SetEnabled 手动启用/禁用订阅。
func (p *SubscriptionPool) SetEnabled(name string, enabled bool) error {
	rt := p.get(name)
	if rt == nil {
		return fmt.Errorf("mihomo-pool: 订阅不存在: %s", name)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.spec.Disabled = !enabled
	if enabled {
		rt.state = SubHealthy
		rt.health.ConsecutiveFail = 0
	} else {
		rt.state = SubDisabled
	}
	return nil
}

// QuotaOf 返回订阅允许的最大在飞比例。
func (p *SubscriptionPool) QuotaOf(name string) float64 {
	rt := p.get(name)
	if rt == nil {
		return 0
	}
	return rt.spec.Quota
}

// mergeHeader 合并请求头，后者的同名键覆盖前者。
func mergeHeader(base, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	out := make(map[string]string, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// maskURL 把 URL 里的敏感部分打码，用于对外展示。
//
// 机场订阅的 token 常见两种位置：query 参数和 path 段。
// 只打码 query 会把 path 里的 token 前 28 个字符露出来，这里两者都打码，
// 对外只保留 scheme + host。
func maskURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "<invalid url>"
	}
	host := u.Host
	if u.User != nil {
		host = "***@" + u.Host
	}
	out := u.Scheme + "://" + host
	if p := u.EscapedPath(); p != "" && p != "/" {
		out += fmt.Sprintf("/<%d chars>", len(p))
	}
	if u.RawQuery != "" {
		out += fmt.Sprintf("?<%d chars>", len(u.RawQuery))
	}
	return out
}

// SortInfosByWeight 按有效权重降序排序，便于观察调度倾向。
func SortInfosByWeight(infos []SubscriptionInfo) {
	sort.SliceStable(infos, func(i, j int) bool {
		if infos[i].EffWeight == infos[j].EffWeight {
			return infos[i].Name < infos[j].Name
		}
		return infos[i].EffWeight > infos[j].EffWeight
	})
}
