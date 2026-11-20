package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 异步高并发执行器。
//
// 语义：
//   - 任务从 channel 进来，固定数量的 worker 消费，goroutine 数受控
//   - 每个请求按策略挑一个节点，占用它的独立端口，互不干扰
//   - 单节点在飞数超过上限时，该节点从候选里被剔除（不是排队等它）
//   - 失败按 Retries 次数重试，每次换一个节点，并记账降权
//   - 结果通过回调或 channel 交付，不无限缓冲

// Request 是一次要执行的 HTTP 请求。
type Request struct {
	// Method 默认 GET。
	Method string
	// URL 必填。
	URL string
	// Header 附加请求头。
	Header map[string]string
	// Body 请求体（读一次用完，重试时自动回放）。
	Body []byte
	// Timeout 单次请求超时，默认 20s。
	Timeout time.Duration
	// Retries 失败后重试次数，默认 1。
	Retries int
	// Filter 限定参与的节点。
	Filter NodeFilter
	// HashKey 给 StrategyHash 用；为空时用 URL。
	HashKey string
	// AllowDirect 所有节点都失败时是否允许直连兜底。默认 false。
	AllowDirect bool
	// Tag 业务标签，原样带回结果。
	Tag string
	// SkipBody 为 true 时只读状态码不读正文。
	SkipBody bool
	// MaxBodyBytes 限制正文读取长度，默认 1 MiB。
	MaxBodyBytes int64
}

// Response 是一次执行的结果。
type Response struct {
	Tag       string
	Status    int
	Header    http.Header
	Body      []byte
	Truncated bool
	// Node 实际使用的节点名；直连兜底时为 "direct"。
	Node string
	// ProxyURL 实际使用的本地代理地址；直连为空。
	ProxyURL string
	// Attempts 一共尝试了几次。
	Attempts int
	// Duration 总耗时（含重试）。
	Duration time.Duration
	// Err 非 nil 表示本次失败。
	Err error
}

// OK 判断是否成功。
func (r *Response) OK() bool {
	return r.Err == nil && r.Status >= 200 && r.Status < 400
}

// ExecutorConfig 是执行器的可调项。
type ExecutorConfig struct {
	// Workers 并发 worker 数，默认 32。
	Workers int
	// PerNode 单节点最大在飞请求数，默认 4（0 表示不限）。
	PerNode int
	// Queue 任务缓冲长度，默认 Workers*4。
	Queue int
	// DefaultTimeout 单个请求的默认超时，默认 20s。
	DefaultTimeout time.Duration
	// RetryBackoff 重试前的等待基数，第 n 次等 n*Backoff，默认 100ms。
	RetryBackoff time.Duration
	// Transport 自定义传输层；为空时按每个代理独立建 Transport。
	Transport func(proxyURL string) http.RoundTripper
}

func (c *ExecutorConfig) normalize() {
	if c.Workers <= 0 {
		c.Workers = 32
	}
	if c.PerNode < 0 {
		c.PerNode = 0
	} else if c.PerNode == 0 {
		c.PerNode = 4
	}
	if c.Queue <= 0 {
		c.Queue = c.Workers * 4
	}
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = 20 * time.Second
	}
	if c.RetryBackoff <= 0 {
		c.RetryBackoff = 100 * time.Millisecond
	}
}

// Executor 是固定的 worker 池。
type Executor struct {
	pool *Pool
	cfg  ExecutorConfig

	jobs   chan *task
	wg     sync.WaitGroup
	closed atomic.Bool

	// 传输层缓存：同一个代理地址复用同一个 Transport，避免连接池碎片化
	trMu sync.Mutex
	trs  map[string]*http.Transport

	// 在飞统计
	inflight atomic.Int64

	// wg/quit 管理 worker 池生命周期；mu 保护 quit 的重建
	mu   sync.Mutex
	quit chan struct{}
}

type task struct {
	req  *Request
	out  chan<- *Response
	done func(*Response)
}

// newExecutor 构造执行器（worker 在第一次 Run/Start 时拉起）。
func newExecutor(p *Pool) *Executor {
	cfg := ExecutorConfig{}
	cfg.normalize()
	e := &Executor{
		pool: p,
		cfg:  cfg,
		trs:  make(map[string]*http.Transport),
	}
	e.jobs = make(chan *task, cfg.Queue)
	e.start()
	return e
}

// Configure 允许在首次使用前调整并发参数。
//
// 调用时会重建队列与 worker 池，所以只能在空闲时调。
func (e *Executor) Configure(cfg ExecutorConfig) {
	if e.closed.Load() || e.inflight.Load() > 0 {
		return
	}
	cfg.normalize()
	e.stopWorkers()
	e.cfg = cfg
	e.jobs = make(chan *task, cfg.Queue)
	e.start()
}

// Start 确保 worker 已拉起。
func (e *Executor) Start() {
	e.start()
}

func (e *Executor) start() {
	e.wg.Add(e.cfg.Workers)
	e.quit = make(chan struct{})
	quit := e.quit
	jobs := e.jobs
	for i := 0; i < e.cfg.Workers; i++ {
		go func() {
			defer e.wg.Done()
			for {
				select {
				case <-quit:
					return
				case t, ok := <-jobs:
					if !ok {
						return
					}
					resp := e.execute(t.req)
					if t.done != nil {
						t.done(resp)
					}
					if t.out != nil {
						t.out <- resp
					}
				}
			}
		}()
	}
}

// stopWorkers 关掉当前 worker 池。
//
// 用 quit 通道而不是 close(jobs)：jobs 可能仍有生产者在写，
// 关闭会直接 panic。worker 收到 quit 后把手上正在处理的请求做完再退出。
func (e *Executor) stopWorkers() {
	e.mu.Lock()
	quit := e.quit
	e.quit = nil
	e.mu.Unlock()
	if quit == nil {
		return
	}
	close(quit)
	e.wg.Wait()
}

// Close 停止执行器。
func (e *Executor) Close() {
	if e.closed.Swap(true) {
		return
	}
	e.stopWorkers()

	e.trMu.Lock()
	for _, tr := range e.trs {
		tr.CloseIdleConnections()
	}
	e.trs = make(map[string]*http.Transport)
	e.trMu.Unlock()
}

// Do 同步执行一个请求（内部仍走 worker 池语义，用于单发场景）。
func (e *Executor) Do(ctx context.Context, req *Request) *Response {
	if e.closed.Load() {
		return &Response{Tag: req.Tag, Err: errors.New("mihomo-pool: Executor 已关闭")}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 直接在当前 goroutine 执行，避免单发场景多一次调度
	return e.executeWithContext(ctx, req)
}

// Submit 异步提交一个请求，返回结果通道（缓冲 1，不会阻塞 worker）。
func (e *Executor) Submit(ctx context.Context, req *Request) (<-chan *Response, error) {
	if e.closed.Load() {
		return nil, errors.New("mihomo-pool: Executor 已关闭")
	}
	out := make(chan *Response, 1)
	t := &task{req: req, out: out}
	select {
	case e.jobs <- t:
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ForEach 批量执行：把 reqs 投递进 worker 池，边完成边回调。
//
// 每个任务自带一条缓冲 1 的结果通道，worker 永不阻塞在共享通道上；
// 在途任务数受 worker 数约束，所以不会一次起 N 个 goroutine。
// 回调在调用方 goroutine 里串行执行。
func (e *Executor) ForEach(ctx context.Context, reqs []*Request, fn func(*Response)) error {
	if len(reqs) == 0 {
		return nil
	}
	if fn == nil {
		fn = func(*Response) {}
	}
	if e.closed.Load() {
		return errors.New("mihomo-pool: Executor 已关闭")
	}

	type pending struct {
		idx int
		ch  <-chan *Response
	}
	inbox := make(chan pending, len(reqs))
	go func() {
		defer close(inbox)
		for i, r := range reqs {
			if r == nil {
				continue
			}
			ch := make(chan *Response, 1)
			t := &task{req: r, out: ch}
			select {
			case e.jobs <- t:
				inbox <- pending{idx: i, ch: ch}
			case <-ctx.Done():
				return
			}
		}
	}()

	var submitErr error
	for p := range inbox {
		select {
		case resp := <-p.ch:
			fn(resp)
		case <-ctx.Done():
			submitErr = ctx.Err()
			// 继续把已投递的任务收完，避免回调和 worker 悬空
			for rest := range inbox {
				<-rest.ch
			}
			return submitErr
		}
	}
	if err := ctx.Err(); err != nil {
		submitErr = err
	}
	return submitErr
}

// RunAsync 是 ForEach 的完全异步版本：返回结果通道，由调用方消费。
//
// 通道容量等于请求数，所以 worker 永远不会因为消费者慢而阻塞。
func (e *Executor) RunAsync(ctx context.Context, reqs []*Request) (<-chan *Response, error) {
	if e.closed.Load() {
		return nil, errors.New("mihomo-pool: Executor 已关闭")
	}
	out := make(chan *Response, len(reqs))
	go func() {
		defer close(out)
		var wg sync.WaitGroup
		sem := make(chan struct{}, e.cfg.Workers*2)
		for _, r := range reqs {
			if r == nil {
				continue
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(req *Request) {
				defer wg.Done()
				defer func() { <-sem }()
				out <- e.executeWithContext(ctx, req)
			}(r)
		}
		wg.Wait()
	}()
	return out, nil
}

// execute 在不带外部 ctx 的情况下执行（给 worker 用）。
func (e *Executor) execute(req *Request) *Response {
	return e.executeWithContext(context.Background(), req)
}

// executeWithContext 是真正的执行逻辑：选节点 → 建 transport → 发请求 → 失败重试换节点。
func (e *Executor) executeWithContext(ctx context.Context, req *Request) *Response {
	start := time.Now()
	resp := &Response{Tag: req.Tag}

	if req == nil || strings.TrimSpace(req.URL) == "" {
		resp.Err = errors.New("mihomo-pool: URL 为空")
		resp.Duration = time.Since(start)
		return resp
	}
	method := req.Method
	if method == "" {
		method = http.MethodGet
	}
	timeout := req.Timeout
	if timeout <= 0 {
		timeout = e.cfg.DefaultTimeout
	}
	retries := req.Retries
	if retries < 0 {
		retries = 0
	}
	maxBody := req.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	hashKey := req.HashKey
	if hashKey == "" {
		hashKey = req.URL
	}

	candidates := e.pool.nodes.Filter(req.Filter)
	var lastErr error

	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			wait := time.Duration(attempt) * e.cfg.RetryBackoff
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				resp.Err = ctx.Err()
				resp.Attempts = attempt
				resp.Duration = time.Since(start)
				return resp
			}
		}

		// 重试轮放宽健康要求：第一次失败后节点已被降权，
		// 若仍按健康过滤会导致唯一节点时永远无候选，重试形同虚设。
		pool := candidates
		if attempt > 0 {
			pool = relaxedCandidates(candidates)
		}
		pick, err := e.pickNode(pool, hashKey)
		if err != nil {
			// 没有可用节点：最后一轮且允许直连就直连兜底
			if req.AllowDirect && attempt == retries {
				out, derr := e.roundTrip(ctx, "", method, req, timeout, maxBody)
				resp.Attempts = attempt + 1
				resp.Node = "direct"
				resp.Duration = time.Since(start)
				if derr != nil {
					resp.Err = derr
					return resp
				}
				// 直连兜底的元信息必须带回，否则调用方看到的是空 Node
				out.Attempts = resp.Attempts
				out.Duration = resp.Duration
				out.Tag = req.Tag
				out.Node = "direct"
				out.ProxyURL = ""
				return out
			}
			lastErr = err
			resp.Attempts = attempt + 1
			continue
		}

		pick.inflight.Add(1)
		e.inflight.Add(1)
		out, perr := e.roundTrip(ctx, pick.ProxyURL, method, req, timeout, maxBody)
		pick.inflight.Add(-1)
		e.inflight.Add(-1)

		// 5xx 视为该节点失败：这类错误几乎总是出口/上游的问题，
		// 换节点重试才有意义。4xx 是请求本身的问题，直接返回。
		if perr == nil && out != nil && out.Status >= 500 && attempt < retries {
			perr = fmt.Errorf("mihomo-pool: 节点 %s 返回 %d", pick.Name, out.Status)
		}

		resp.Attempts = attempt + 1
		resp.Node = pick.Name
		resp.ProxyURL = pick.ProxyURL
		resp.Duration = time.Since(start)

		if perr == nil && out != nil {
			pick.okCount.Add(1)
			out.Tag = req.Tag
			out.Node = pick.Name
			out.ProxyURL = pick.ProxyURL
			out.Attempts = resp.Attempts
			out.Duration = resp.Duration
			return out
		}

		// 失败：记账降权，下一轮自然排到后面
		pick.failCount.Add(1)
		pick.lastFail.Store(time.Now().UnixNano())
		if perr != nil {
			lastErr = perr
		}
	}

	resp.Err = lastErr
	if resp.Err == nil {
		resp.Err = errors.New("mihomo-pool: 请求失败（所有节点）")
	}
	resp.Duration = time.Since(start)
	return resp
}

// pickNode 按在飞上限与策略选节点。
//
// 单节点在飞数达到 PerNode 上限时，从候选里剔除；
// 全被剔除说明负载已满，返回错误让调用方退避而不是继续堆。
func (e *Executor) pickNode(candidates []*Node, key string) (*Node, error) {
	if len(candidates) == 0 {
		return nil, errors.New("mihomo-pool: 筛选后没有候选节点")
	}
	usable := candidates
	if e.cfg.PerNode > 0 {
		usable = make([]*Node, 0, len(candidates))
		for _, n := range candidates {
			if n.Inflight() < int64(e.cfg.PerNode) {
				usable = append(usable, n)
			}
		}
		if len(usable) == 0 {
			return nil, fmt.Errorf("mihomo-pool: 全部节点在飞已满（每节点上限 %d）", e.cfg.PerNode)
		}
	}
	return e.pool.disp.Pick(usable, key)
}

// roundTrip 用指定代理发一次请求。
func (e *Executor) roundTrip(ctx context.Context, proxyURL, method string, req *Request, timeout time.Duration, maxBody int64) (*Response, error) {
	tr, err := e.transport(proxyURL)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Transport: tr, Timeout: timeout}

	var bodyReader io.Reader
	if len(req.Body) > 0 {
		bodyReader = strings.NewReader(string(req.Body))
	}

	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	hreq, err := http.NewRequestWithContext(reqCtx, method, req.URL, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("mihomo-pool: 构造请求失败: %w", err)
	}
	for k, v := range req.Header {
		hreq.Header.Set(k, v)
	}
	if hreq.Header.Get("User-Agent") == "" {
		hreq.Header.Set("User-Agent", "mihomo-pool/1.0")
	}
	if hreq.Header.Get("Accept") == "" {
		hreq.Header.Set("Accept", "*/*")
	}

	resp, err := client.Do(hreq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := &Response{Status: resp.StatusCode, Header: resp.Header}
	if req.SkipBody {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return out, nil
	}
	b, rerr := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	if rerr != nil {
		return nil, fmt.Errorf("mihomo-pool: 读响应失败: %w", rerr)
	}
	if int64(len(b)) > maxBody {
		b = b[:maxBody]
		out.Truncated = true
	}
	out.Body = b
	return out, nil
}

// transport 按代理地址拿一个缓存的 Transport。
//
// 每个本地端口在 mihomo 里对应固定出站，所以按代理地址缓存是安全的：
// 同一个端口永远走同一个节点。直连（空串）用无代理 Transport。
func (e *Executor) transport(proxyURL string) (*http.Transport, error) {
	e.trMu.Lock()
	if tr, ok := e.trs[proxyURL]; ok {
		e.trMu.Unlock()
		return tr, nil
	}
	e.trMu.Unlock()

	var tr *http.Transport
	if e.cfg.Transport != nil {
		if rt := e.cfg.Transport(proxyURL); rt != nil {
			if t, ok := rt.(*http.Transport); ok {
				tr = t
			} else {
				// 非 *http.Transport 的自定义传输层：用默认 transport 包一层代理
				tr = defaultTransport()
			}
		}
	}
	if tr == nil {
		tr = defaultTransport()
	}

	if strings.TrimSpace(proxyURL) != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("mihomo-pool: 代理地址无效 %q: %w", proxyURL, err)
		}
		switch u.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return nil, fmt.Errorf("mihomo-pool: 不支持的代理协议 %q", u.Scheme)
		}
		tr.Proxy = http.ProxyURL(u)
	} else {
		tr.Proxy = nil
	}

	e.trMu.Lock()
	if old, ok := e.trs[proxyURL]; ok {
		e.trMu.Unlock()
		tr.CloseIdleConnections()
		return old, nil
	}
	e.trs[proxyURL] = tr
	e.trMu.Unlock()
	return tr, nil
}

// defaultTransport 返回一个适合高并发的 Transport。
func defaultTransport() *http.Transport {
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       60 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    false,
		DisableKeepAlives:     false,
	}
}

// Stats 返回执行器统计。
type Stats struct {
	Workers  int
	Queue    int64
	Inflight int64
}

// Stats 返回统计快照。
func (e *Executor) Stats() Stats {
	return Stats{
		Workers:  e.cfg.Workers,
		Queue:    int64(len(e.jobs)),
		Inflight: e.inflight.Load(),
	}
}

var _ = strconv.Itoa

// relaxedCandidates 返回重试轮可用的候选。
//
// 健康节点优先；若一个都没有，就把全部候选放进来（含刚被降权的节点），
// 这样"只有一个节点、它刚失败"的场景仍有办法再试一次。
func relaxedCandidates(candidates []*Node) []*Node {
	healthy := make([]*Node, 0, len(candidates))
	for _, n := range candidates {
		if n != nil && n.Healthy() {
			healthy = append(healthy, n)
		}
	}
	if len(healthy) > 0 {
		return healthy
	}
	out := make([]*Node, 0, len(candidates))
	for _, n := range candidates {
		if n != nil && n.Alive {
			out = append(out, n)
		}
	}
	if len(out) > 0 {
		return out
	}
	return candidates
}
