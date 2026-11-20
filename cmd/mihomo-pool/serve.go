package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// serve：常驻 JSONL 服务。
//
// 为什么用常驻进程而不是每次起一个 CLI：
//   - 内核启动要 100ms~2s，每次调用都付这个代价不可接受
//   - 连接池、TLS 会话、节点延迟表都需要跨请求复用
//   - 一个进程内 worker 池能精确控制并发，Python 侧只需投递任务
//
// 并发模型：
//   - 主 goroutine 阻塞读 stdin，读到请求就交给执行器（非阻塞）
//   - 执行器回调把结果序列化后写入 stdout，用互斥锁保证整行原子
//   - 读满 N 个在飞（-max-inflight）就暂停读，形成背压，不会打爆内存
func runServe(argv []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)

	var (
		workers     int
		perNode     int
		maxInflight int
		strategy    string
		subURL      string
		nodesFile   string
		providers   []string
		noKernel    bool
		keepInfo    bool
		keepDupes   bool
		idleTimeout time.Duration
	)
	fs.IntVar(&workers, "workers", 32, "并发 worker 数")
	fs.IntVar(&perNode, "per-node", 4, "单节点最大在飞请求数")
	fs.IntVar(&maxInflight, "max-inflight", 0, "最多在途请求数（0=workers*8），形成背压")
	fs.StringVar(&strategy, "strategy", string(mihomopool.StrategyLeastInflight), "分发策略")
	fs.StringVar(&subURL, "sub", "", "订阅地址，启动时拉取并生成节点")
	fs.StringVar(&nodesFile, "nodes", "", "节点 JSON 文件（[{\"name\",\"type\",\"server\",\"port\",\"raw\"}]）")
	fs.Func("provider", "订阅 URL，交给内核对拉，可重复（格式 name=url[|interval]）", func(v string) error {
		providers = append(providers, v)
		return nil
	})
	fs.BoolVar(&noKernel, "no-kernel", false, "不启动内核（已有内核在别处跑，只做请求分发）")
	fs.BoolVar(&keepInfo, "keep-info-nodes", false, "保留机场投放的信息节点（剩余流量/套餐到期等，默认丢弃）")
	fs.BoolVar(&keepDupes, "keep-duplicates", false, "保留连接参数完全相同的重复节点（默认丢弃）")
	fs.DurationVar(&idleTimeout, "idle-timeout", 0, "空闲多久自动退出（0=不限）")

	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, err := mihomopool.New(cf.toOptions())
	if err != nil {
		return fail(err.Error(), nil)
	}

	if !noKernel {
		nodes, perr := loadNodes(ctx, subURL, nodesFile, mihomopool.ParseOptions{
			KeepInfoNodes:  keepInfo,
			KeepDuplicates: keepDupes,
		})
		if perr != nil {
			return fail("加载节点失败: "+perr.Error(), nil)
		}
		specs := parseProviders(providers)

		if err := pool.Start(ctx, nodes, specs); err != nil {
			return fail("启动内核失败: "+err.Error(), map[string]any{
				"hint": "检查 -bin 指向的 mihomo 是否存在、-base 起的端口是否空闲",
			})
		}
		defer func() {
			sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
			defer sc()
			_ = pool.Stop(sctx)
		}()
	} else {
		// 外部内核：直接连控制面同步节点，不启动进程
		if err := pool.Attach(ctx); err != nil {
			return fail("连接外部内核失败: "+err.Error(), nil)
		}
	}

	if err := pool.SetStrategy(mihomopool.Strategy(strategy)); err != nil {
		return fail(err.Error(), map[string]any{
			"hint": "可用策略: least-inflight, lowest-latency, round-robin, random, hash, weighted",
		})
	}
	pool.ConfigureExecutor(workers, perNode)

	if maxInflight <= 0 {
		maxInflight = workers * 8
	}

	s := &server{
		pool:        pool,
		maxInflight: maxInflight,
		sem:         make(chan struct{}, maxInflight),
		outMu:       &sync.Mutex{},
		writer:      bufio.NewWriterSize(os.Stdout, 1<<20),
		idleTimeout: idleTimeout,
		done:        make(chan struct{}),
	}

	// 握手：告诉 Python 端协议版本、内核状态、可用节点
	st := pool.Status(ctx)
	s.send(&responseMsg{
		Kind: "hello",
		OK:   true,
		Op:   "hello",
		Data: map[string]any{
			"protocol":      protocolVersion,
			"cli":           buildVersion,
			"lib":           mihomopool.Version,
			"state":         string(st.State),
			"version":       st.Version,
			"nodes":         st.Nodes,
			"healthy":       st.Healthy,
			"ports":         st.Ports,
			"strategy":      st.Strategy,
			"workers":       workers,
			"per_node":      perNode,
			"max_inflight":  maxInflight,
			"default_proxy": pool.DefaultProxyURL(),
		},
	})
	s.flush()

	s.serveLoop(ctx)
	s.waitInflight()
	s.flush()
	return 0
}

// loadNodes 从订阅或文件加载节点。
func loadNodes(ctx context.Context, subURL, nodesFile string, po mihomopool.ParseOptions) ([]mihomopool.NodeSpec, error) {
	if subURL != "" {
		return mihomopool.FetchSubscription(ctx, subURL, mihomopool.FetchOptions{Parse: po})
	}
	if nodesFile != "" {
		nodes, err := readNodesFile(nodesFile)
		if err != nil {
			return nil, err
		}
		return mihomopool.CleanNodes(nodes, po), nil
	}
	return nil, nil
}

// readNodesFile 从 JSON 文件读节点定义。
func readNodesFile(path string) ([]mihomopool.NodeSpec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		Name   string         `json:"name"`
		Type   string         `json:"type"`
		Server string         `json:"server"`
		Port   int            `json:"port"`
		Group  string         `json:"group"`
		Raw    map[string]any `json:"raw"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("解析节点文件失败: %w", err)
	}
	out := make([]mihomopool.NodeSpec, 0, len(raw))
	for _, r := range raw {
		out = append(out, mihomopool.NodeSpec{
			Name: r.Name, Type: r.Type, Server: r.Server, Port: r.Port, Group: r.Group, Raw: r.Raw,
		})
	}
	return out, nil
}

// parseProviders 解析 name=url[|interval] 形式。
func parseProviders(items []string) []mihomopool.ProviderSpec {
	out := make([]mihomopool.ProviderSpec, 0, len(items))
	for i, item := range items {
		item = trimNL(item)
		if item == "" {
			continue
		}
		name := fmt.Sprintf("provider%d", i+1)
		rest := item
		if idx := indexByte(item, '='); idx > 0 {
			name = item[:idx]
			rest = item[idx+1:]
		}
		interval := 0
		if idx := indexByte(rest, '|'); idx > 0 {
			fmt.Sscanf(rest[idx+1:], "%d", &interval)
			rest = rest[:idx]
		}
		out = append(out, mihomopool.ProviderSpec{Name: name, URL: rest, IntervalSec: interval})
	}
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// server 是常驻服务的运行时状态。
type server struct {
	pool        *mihomopool.Pool
	maxInflight int
	sem         chan struct{}
	outMu       *sync.Mutex
	writer      *bufio.Writer
	idleTimeout time.Duration
	done        chan struct{}

	inflightMu   sync.Mutex
	inflightWG   sync.WaitGroup
	lastActivity time.Time
}

// send 序列化并写出一行，整行原子。
func (s *server) send(msg *responseMsg) {
	b, err := json.Marshal(msg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mihomo-pool: 序列化响应失败: %v\n", err)
		return
	}
	s.outMu.Lock()
	s.writer.Write(b)
	s.writer.WriteByte('\n')
	s.outMu.Unlock()
}

// flush 把缓冲刷出去。
func (s *server) flush() {
	s.outMu.Lock()
	_ = s.writer.Flush()
	s.outMu.Unlock()
}

// serveLoop 读 stdin 直到 EOF。
func (s *server) serveLoop(ctx context.Context) {
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20) // 单行最大 64MB（base64 body 可能很大）

	for sc.Scan() {
		line := trimNL(sc.Text())
		if line == "" {
			continue
		}
		select {
		case <-ctx.Done():
			return
		default:
		}

		var msg requestMsg
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			s.send(&responseMsg{
				Kind: "err", OK: false,
				Error: fmt.Sprintf("JSON 解析失败: %v", err),
			})
			s.flush()
			continue
		}

		if done := s.handle(ctx, &msg); done {
			return
		}
		// 每处理一行刷一次，Python 侧 readline 才能及时拿到
		s.flush()
	}

	if err := sc.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "mihomo-pool: 读 stdin 失败: %v\n", err)
	}
}

// handle 分派一个消息。返回 true 表示要求退出。
func (s *server) handle(ctx context.Context, msg *requestMsg) bool {
	op := msg.Op
	if op == "" {
		op = "request"
	}

	switch op {
	case "request", "http", "fetch":
		s.dispatchRequest(ctx, msg)
		return false

	case "ping":
		s.send(&responseMsg{ID: msg.ID, Op: "ping", Kind: "ack", OK: true,
			Data: map[string]any{"pong": time.Now().UnixMilli()}})
		return false

	case "status":
		st := s.pool.Status(ctx)
		s.send(&responseMsg{ID: msg.ID, Op: "status", Kind: "ack", OK: true, Data: map[string]any{
			"state": string(st.State), "pid": st.PID, "version": st.Version,
			"nodes": st.Nodes, "healthy": st.Healthy, "ports": st.Ports,
			"strategy": string(st.Strategy), "config": st.Config,
			"last_exit": st.LastExit, "restarts": st.Restarts,
		}})
		return false

	case "nodes":
		list := s.pool.FindNodes(nodeFilterFromArgs(msg.Args))
		s.send(&responseMsg{ID: msg.ID, Op: "nodes", Kind: "ack", OK: true,
			Data: map[string]any{"nodes": nodesToData(list), "total": len(list)}})
		return false

	case "refresh":
		if err := s.pool.RefreshNodes(ctx); err != nil {
			s.send(&responseMsg{ID: msg.ID, Op: "refresh", Kind: "err", OK: false, Error: err.Error()})
			return false
		}
		s.send(&responseMsg{ID: msg.ID, Op: "refresh", Kind: "ack", OK: true,
			Data: map[string]any{"nodes": s.pool.Nodes().Len()}})
		return false

	case "health":
		concurrency := argInt(msg.Args, "concurrency", 16)
		hctx, hc := ctxWithTimeout(ctx, 2*time.Minute)
		defer hc()
		h, err := s.pool.CheckHealth(hctx, concurrency)
		if err != nil {
			s.send(&responseMsg{ID: msg.ID, Op: "health", Kind: "err", OK: false, Error: err.Error()})
			return false
		}
		detail := make([]map[string]any, 0, len(h.Detail))
		for _, d := range h.Detail {
			detail = append(detail, map[string]any{
				"node": d.Node, "delay_ms": d.DelayMS, "ok": d.OK, "error": d.Error,
			})
		}
		s.send(&responseMsg{ID: msg.ID, Op: "health", Kind: "ack", OK: true, Data: map[string]any{
			"ok": h.OK, "state": string(h.State), "version": h.Version,
			"nodes": h.Nodes, "healthy": h.Healthy, "detail": detail,
		}})
		return false

	case "stats":
		es := s.pool.Executor().Stats()
		st := s.pool.Status(ctx)
		s.send(&responseMsg{ID: msg.ID, Op: "stats", Kind: "ack", OK: true, Data: map[string]any{
			"workers": es.Workers, "queue": es.Queue, "inflight": es.Inflight,
			"nodes": st.Nodes, "healthy": st.Healthy,
		}})
		return false

	case "strategy":
		name := argString(msg.Args, "strategy")
		if name == "" {
			name = argString(msg.Args, "name")
		}
		if err := s.pool.SetStrategy(mihomopool.Strategy(name)); err != nil {
			s.send(&responseMsg{ID: msg.ID, Op: "strategy", Kind: "err", OK: false, Error: err.Error()})
			return false
		}
		s.send(&responseMsg{ID: msg.ID, Op: "strategy", Kind: "ack", OK: true,
			Data: map[string]any{"strategy": name}})
		return false

	case "close", "quit", "exit":
		s.send(&responseMsg{ID: msg.ID, Op: "close", Kind: "ack", OK: true})
		return true

	default:
		s.send(&responseMsg{ID: msg.ID, Op: op, Kind: "err", OK: false,
			Error: fmt.Sprintf("未知 op %q（可用: request/status/nodes/refresh/health/stats/strategy/ping/close）", op)})
		return false
	}
}

// dispatchRequest 把请求交给执行器并发执行，结果异步写回。
func (s *server) dispatchRequest(ctx context.Context, msg *requestMsg) {
	lreq, err := msg.toLibraryRequest()
	if err != nil {
		s.send(&responseMsg{ID: msg.ID, Op: "request", Kind: "err", OK: false, Error: err.Error()})
		return
	}

	// 背压：在途请求达到 maxInflight 就在这里等，不再往下读
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}

	s.inflightWG.Add(1)
	go func() {
		defer s.inflightWG.Done()
		defer func() { <-s.sem }()

		resp := s.pool.Executor().Do(ctx, lreq)
		out := fromLibraryResponse(msg.ID, resp)
		s.send(out)
		s.flush()
	}()
}

// waitInflight 等所有在途请求收尾。
func (s *server) waitInflight() {
	done := make(chan struct{})
	go func() {
		s.inflightWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		fmt.Fprintln(os.Stderr, "mihomo-pool: 等待在途请求超时，强制退出")
	}
}
