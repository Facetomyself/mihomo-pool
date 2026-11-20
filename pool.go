package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 分发策略。
//
// 每个请求从候选节点里挑一个，挑法由策略决定。所有策略都建立在
// NodeSet.Filter 的结果上，且都会避开"在飞数已满"的节点。

// Strategy 是节点选择策略。
type Strategy string

const (
	// StrategyLeastInflight 最少在飞优先，同分按综合分（等价最少连接数）。
	StrategyLeastInflight Strategy = "least-inflight"
	// StrategyLowestLatency 最低延迟优先。
	StrategyLowestLatency Strategy = "lowest-latency"
	// StrategyRoundRobin 轮询。
	StrategyRoundRobin Strategy = "round-robin"
	// StrategyRandom 随机。
	StrategyRandom Strategy = "random"
	// StrategyHash 按 Key 一致性哈希，同一 Key 固定落同一节点。
	StrategyHash Strategy = "hash"
	// StrategyWeighted 按健康分加权随机（分越高权重越大）。
	StrategyWeighted Strategy = "weighted"
)

// ParseStrategy 把字符串转成策略，未知值报错。
func ParseStrategy(s string) (Strategy, error) {
	switch Strategy(s) {
	case StrategyLeastInflight, StrategyLowestLatency, StrategyRoundRobin, StrategyRandom, StrategyHash, StrategyWeighted:
		return Strategy(s), nil
	}
	return "", fmt.Errorf("mihomo-pool: 未知分发策略 %q", s)
}

// Dispatcher 在候选节点间做选择。
type Dispatcher struct {
	strategy Strategy
	cursor   atomic.Uint64
	mu       sync.Mutex
	rnd      *rand.Rand
}

// NewDispatcher 构造分发器。
func NewDispatcher(s Strategy) (*Dispatcher, error) {
	if s == "" {
		s = StrategyLeastInflight
	}
	if _, err := ParseStrategy(string(s)); err != nil {
		return nil, err
	}
	return &Dispatcher{
		strategy: s,
		rnd:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}, nil
}

// Strategy 返回当前策略。
func (d *Dispatcher) Strategy() Strategy { return d.strategy }

// Pick 从候选里挑一个节点。candidates 已按 Score 升序。
//
// key 只在 StrategyHash 下使用。
func (d *Dispatcher) Pick(candidates []*Node, key string) (*Node, error) {
	alive := make([]*Node, 0, len(candidates))
	for _, n := range candidates {
		if n != nil && n.Healthy() {
			alive = append(alive, n)
		}
	}
	if len(alive) == 0 {
		return nil, errors.New("mihomo-pool: 无可用节点")
	}

	switch d.strategy {
	case StrategyLeastInflight:
		// 不依赖调用方预排序：在可用集合里按 Score 取最小
		best := alive[0]
		for _, n := range alive[1:] {
			if n.Score() < best.Score() {
				best = n
			}
		}
		return best, nil

	case StrategyLowestLatency:
		best := alive[0]
		for _, n := range alive[1:] {
			if n.DelayMS > 0 && (best.DelayMS <= 0 || n.DelayMS < best.DelayMS) {
				best = n
			}
		}
		return best, nil

	case StrategyRoundRobin:
		i := d.cursor.Add(1) - 1
		return alive[int(i%uint64(len(alive)))], nil

	case StrategyRandom:
		d.mu.Lock()
		i := d.rnd.Intn(len(alive))
		d.mu.Unlock()
		return alive[i], nil

	case StrategyHash:
		h := fnv.New64a()
		_, _ = h.Write([]byte(key))
		return alive[int(h.Sum64()%uint64(len(alive)))], nil

	case StrategyWeighted:
		total := 0.0
		weights := make([]float64, len(alive))
		for i, n := range alive {
			// 分数越低权重越大，下限 1 避免饿死
			w := 1000.0 / float64(max64(n.Score(), 1))
			weights[i] = w
			total += w
		}
		d.mu.Lock()
		r := d.rnd.Float64() * total
		d.mu.Unlock()
		acc := 0.0
		for i, w := range weights {
			acc += w
			if r <= acc {
				return alive[i], nil
			}
		}
		return alive[len(alive)-1], nil
	}
	return alive[0], nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Pool 把 Kernel、Control、NodeSet 和 Dispatcher 组合成对外入口。
type Pool struct {
	opts   Options
	kernel *Kernel
	ctl    *Control
	nodes  *NodeSet
	disp   *Dispatcher
	exec   *Executor

	mu        sync.Mutex
	started   bool
	nodesSpec []NodeSpec
	providers []ProviderSpec
	proxyBase string // 默认共享端口的本地 URL，无节点时兜底
}

// New 构造 Pool，不启动内核。
func New(o Options) (*Pool, error) {
	d := DefaultOptions()
	// 把用户没填的字段补成默认值
	if o.ConfigFile == "" {
		o.ConfigFile = d.ConfigFile
	}
	if err := o.normalize(); err != nil {
		return nil, err
	}
	disp, err := NewDispatcher(StrategyLeastInflight)
	if err != nil {
		return nil, err
	}
	p := &Pool{
		opts:   o,
		kernel: newKernel(o),
		ctl:    newControl(o),
		nodes:  newNodeSet(),
		disp:   disp,
	}
	p.exec = newExecutor(p)
	return p, nil
}

// Options 返回当前生效选项的副本。
func (p *Pool) Options() Options { return p.opts }

// Kernel 返回内核句柄（高级用法）。
func (p *Pool) Kernel() *Kernel { return p.kernel }

// Control 返回控制面客户端（高级用法）。
func (p *Pool) Control() *Control { return p.ctl }

// Nodes 返回节点集合（高级用法）。
func (p *Pool) Nodes() *NodeSet { return p.nodes }

// SetStrategy 切换分发策略。
func (p *Pool) SetStrategy(s Strategy) error {
	disp, err := NewDispatcher(s)
	if err != nil {
		return err
	}
	p.disp = disp
	return nil
}

// Start 生成配置、拉起内核、同步节点。
//
// nodes 里的节点各自分到 BasePort+i 的独立端口；
// providers 里的订阅交给内核对拉，节点名在 Start 之后从控制面同步。
func (p *Pool) Start(ctx context.Context, nodes []NodeSpec, providers []ProviderSpec) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("mihomo-pool: Pool 已启动")
	}
	p.mu.Unlock()

	if err := p.kernel.Start(ctx, nodes, providers); err != nil {
		return err
	}

	p.mu.Lock()
	p.started = true
	p.nodesSpec = nodes
	p.providers = providers
	p.proxyBase = p.opts.localProxyURL(p.opts.SocksPort)
	p.mu.Unlock()

	// 首次同步节点；控制面刚起来时 /proxies 可能还没填充 provider 节点，重试几次
	if err := p.syncNodes(ctx, 5); err != nil {
		return fmt.Errorf("mihomo-pool: 内核已起但节点同步失败: %w", err)
	}
	return nil
}

// syncNodes 从控制面拉节点，带重试。
func (p *Pool) syncNodes(ctx context.Context, attempts int) error {
	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-time.After(time.Duration(i) * 200 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		bindings := p.kernel.Bindings()
		if err := p.nodes.Refresh(ctx, p.ctl, bindings); err != nil {
			lastErr = err
			continue
		}
		if p.nodes.Len() > 0 {
			return nil
		}
		lastErr = errors.New("控制面暂无节点")
	}
	return lastErr
}

// Reload 用新节点集合重载配置。
//
// 非 Windows 走 SIGHUP；Windows 没有等价信号，走控制面 PUT /configs。
// provider 节点需要重载后重新从控制面同步。
func (p *Pool) Reload(ctx context.Context, nodes []NodeSpec, providers []ProviderSpec) error {
	if err := p.kernel.Reload(nodes, providers); err != nil {
		cfg := p.kernel.Status().ConfigPath
		if cfg == "" {
			return err
		}
		if e2 := p.ctl.SetConfigPath(ctx, cfg); e2 != nil {
			return fmt.Errorf("mihomo-pool: 重载失败（信号: %v；控制面: %v）", err, e2)
		}
	}
	p.mu.Lock()
	p.nodesSpec = nodes
	p.providers = providers
	p.mu.Unlock()
	return p.syncNodes(ctx, 5)
}

// Stop 停止内核。
func (p *Pool) Stop(ctx context.Context) error {
	p.mu.Lock()
	p.started = false
	p.mu.Unlock()
	if p.exec != nil {
		p.exec.Close()
	}
	return p.kernel.Stop(ctx)
}

// Status 返回整体状态。
type Status struct {
	State    ProcessState
	PID      int
	Version  string
	Nodes    int
	Healthy  int
	Ports    int
	Strategy Strategy
	Config   string
	LastExit string
	Restarts int
}

// Status 汇总状态。
func (p *Pool) Status(ctx context.Context) Status {
	ks := p.kernel.Status()
	st := Status{
		State:    ks.State,
		PID:      ks.PID,
		Strategy: p.disp.Strategy(),
		Config:   ks.ConfigPath,
		LastExit: ks.LastExit,
		Restarts: ks.Restarts,
	}
	all := p.nodes.All()
	st.Nodes = len(all)
	for _, n := range all {
		if n.Healthy() {
			st.Healthy++
		}
		if n.Port > 0 {
			st.Ports++
		}
	}
	if ks.State == StateRunning {
		if v, err := p.ctl.Version(ctx); err == nil {
			st.Version = v
		}
	}
	return st
}

// Health 是对外的一次体检结果。
type Health struct {
	OK      bool
	State   ProcessState
	Version string
	Nodes   int
	Healthy int
	Detail  []DelayResult
}

// CheckHealth 触发一次并发测速并返回体检结果。
func (p *Pool) CheckHealth(ctx context.Context, concurrency int) (Health, error) {
	all := p.nodes.All()
	names := make([]string, 0, len(all))
	for _, n := range all {
		names = append(names, n.Name)
	}
	res := p.nodes.DelayAll(ctx, p.ctl, names, DefaultDelayTestURL, 5*time.Second, concurrency)

	h := Health{State: p.kernel.Status().State, Nodes: len(all)}
	h.Detail = res
	for _, r := range res {
		if r.OK {
			h.Healthy++
		}
	}
	if v, err := p.ctl.Version(ctx); err == nil {
		h.Version = v
	}
	h.OK = h.State == StateRunning && h.Healthy > 0
	return h, nil
}

// FindNodes 按条件筛节点。
func (p *Pool) FindNodes(f NodeFilter) []*Node {
	return p.nodes.Filter(f)
}

// RefreshNodes 从控制面重新同步节点列表。
func (p *Pool) RefreshNodes(ctx context.Context) error {
	return p.syncNodes(ctx, 3)
}

// ProxyURLFor 返回某个节点对应的本地代理地址。
//
// 有独立端口的节点返回其专属地址；provider 节点返回空串（没有独立端口）。
func (p *Pool) ProxyURLFor(nodeName string) string {
	if n, ok := p.nodes.Get(nodeName); ok && n.ProxyURL != "" {
		return n.ProxyURL
	}
	return ""
}

// DefaultProxyURL 返回共享端口的本地代理地址。
//
// 走共享端口的流量由内核规则 + selector 决定出口，不保证落在某个节点上；
// 需要确定出口请用 ProxyURLFor。
func (p *Pool) DefaultProxyURL() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.proxyBase
}

// Executor 返回执行器。
func (p *Pool) Executor() *Executor { return p.exec }

// ConfigureExecutor 调整并发参数（空闲时生效）。
//
// workers 是并发请求数，perNode 是单节点在飞上限。
func (p *Pool) ConfigureExecutor(workers, perNode int) {
	p.exec.Configure(ExecutorConfig{Workers: workers, PerNode: perNode})
}

// WaitReady 阻塞到内核就绪（用于测试与启动编排）。
func (p *Pool) WaitReady(ctx context.Context) error {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if p.kernel.Status().State == StateRunning {
				if err := p.ctl.Pinger(ctx); err == nil {
					return nil
				}
			}
		}
	}
}
