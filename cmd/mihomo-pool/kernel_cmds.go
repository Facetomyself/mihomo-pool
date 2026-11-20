package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// runStart：启动内核并常驻，输出端口信息到 stdout 后阻塞。
//
// 用途：让 mihomo 内核由这个进程托管，Python 用 -no-kernel 方式连上来用。
// 收到 SIGINT/SIGTERM 时优雅停止内核再退出。
func runStart(argv []string) int {
	fs := flag.NewFlagSet("start", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)

	var (
		subURL    string
		nodesFile string
		providers multiFlag
		strategy  string
		workers   int
		perNode   int
		daemonOut string
	)
	fs.StringVar(&subURL, "sub", "", "订阅地址，启动时拉取并生成节点")
	fs.StringVar(&nodesFile, "nodes", "", "节点 JSON 文件")
	fs.Var(&providers, "provider", "订阅交给内核对拉，格式 name=url[|interval]，可重复")
	fs.StringVar(&strategy, "strategy", string(mihomopool.StrategyLeastInflight), "分发策略")
	fs.IntVar(&workers, "workers", 32, "并发 worker 数")
	fs.IntVar(&perNode, "per-node", 4, "单节点在飞上限")
	fs.StringVar(&daemonOut, "ready-file", "", "就绪后把状态 JSON 写到这个文件")

	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, err := mihomopool.New(cf.toOptions())
	if err != nil {
		return fail(err.Error(), nil)
	}

	nodes, nerr := loadNodes(ctx, subURL, nodesFile, mihomopool.ParseOptions{})
	if nerr != nil {
		return fail("加载节点失败: "+nerr.Error(), nil)
	}

	if err := pool.Start(ctx, nodes, parseProviders(providers)); err != nil {
		return fail("启动内核失败: "+err.Error(), map[string]any{
			"hint": "检查 -bin 指向的 mihomo、-base 起的端口是否空闲",
		})
	}
	if err := pool.SetStrategy(mihomopool.Strategy(strategy)); err != nil {
		return fail(err.Error(), nil)
	}
	pool.ConfigureExecutor(workers, perNode)

	st := pool.Status(ctx)
	ports := map[string]any{
		"socks":  cf.socks,
		"api":    cf.api,
		"base":   cf.base,
		"shared": pool.DefaultProxyURL(),
	}
	nodePorts := map[string]any{}
	for _, n := range pool.FindNodes(mihomopool.NodeFilter{RequirePort: true}) {
		nodePorts[n.Name] = n.ProxyURL
	}

	ready := map[string]any{
		"ok":         true,
		"kind":       "ready",
		"pid":        os.Getpid(),
		"kernel_pid": st.PID,
		"version":    st.Version,
		"state":      string(st.State),
		"nodes":      st.Nodes,
		"ports":      ports,
		"node_ports": nodePorts,
		"protocol":   protocolVersion,
	}
	emit(ready)

	if daemonOut != "" {
		b, _ := jsonMarshalIndent(ready)
		_ = os.WriteFile(daemonOut, b, 0o644)
	}

	// 阻塞直到收到信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	select {
	case <-sigCh:
	case <-ctx.Done():
	}

	sctx, sc := context.WithTimeout(context.Background(), 20*time.Second)
	defer sc()
	if err := pool.Stop(sctx); err != nil {
		return fail("停止失败: "+err.Error(), nil)
	}
	emit(map[string]any{"ok": true, "kind": "stopped"})
	return 0
}

// runStatus：查内核状态。
func runStatus(argv []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)
	pretty := fs.Bool("pretty", false, "缩进输出")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, err := mihomopool.New(cf.toOptions())
	if err != nil {
		return fail(err.Error(), nil)
	}

	// 状态查询连控制面即可，不启动进程
	if err := pool.Attach(ctx); err != nil {
		writeJSON(map[string]any{
			"ok":    false,
			"kind":  "status",
			"error": err.Error(),
			"hint":  "内核没在跑？用 start 子命令起一个，或检查 -api 端口",
		}, *pretty)
		return 1
	}

	st := pool.Status(ctx)
	writeJSON(map[string]any{
		"ok":        true,
		"kind":      "status",
		"state":     string(st.State),
		"pid":       st.PID,
		"version":   st.Version,
		"nodes":     st.Nodes,
		"healthy":   st.Healthy,
		"ports":     st.Ports,
		"strategy":  string(st.Strategy),
		"config":    st.Config,
		"last_exit": st.LastExit,
		"restarts":  st.Restarts,
		"shared":    pool.DefaultProxyURL(),
	}, *pretty)
	return 0
}

// runStop：停内核。
//
// 内核是独立进程，这里通过控制面判断是否在跑；
// 真正的停止靠给 start 进程发信号——所以 stop 的作用是：
//   - 若本机有 PID 文件，读出来发 SIGTERM
//   - 否则只能报告「没有可停的内核」
func runStop(argv []string) int {
	fs := flag.NewFlagSet("stop", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)
	pidFile := fs.String("pid-file", "", "start 子命令写下的 PID 文件")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	// 先探一下控制面在不在
	probe, err := mihomopool.New(cf.toOptions())
	if err == nil {
		if aerr := probe.Attach(ctx); aerr == nil {
			// 在跑，但停不了别人起的进程
			if *pidFile == "" {
				return fail("内核在跑，但缺少 -pid-file 无法定位托管进程", map[string]any{
					"hint": "内核由 start 子命令托管。用 start -ready-file 记录状态，或直接给托管进程发 SIGTERM",
				})
			}
		}
	}

	if *pidFile == "" {
		return fail("缺少 -pid-file", nil)
	}

	pid, perr := readPIDFile(*pidFile)
	if perr != nil {
		return fail("读 PID 文件失败: "+perr.Error(), nil)
	}
	proc, perr := os.FindProcess(pid)
	if perr != nil {
		return fail("找不到进程: "+perr.Error(), nil)
	}
	if serr := proc.Signal(syscall.SIGTERM); serr != nil {
		return fail("发送 SIGTERM 失败: "+serr.Error(), nil)
	}
	writeJSON(map[string]any{"ok": true, "kind": "stopped", "pid": pid}, false)
	return 0
}

// runHealth：并发测速。
func runHealth(argv []string) int {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)
	concurrency := fs.Int("concurrency", 16, "并发测速数")
	pretty := fs.Bool("pretty", false, "缩进输出")
	subURL := fs.String("sub", "", "订阅地址：拉取后直接起一个临时内核测速（用完即停）")
	nodesFile := fs.String("nodes", "", "节点 JSON 文件")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, stop, err := openPool(ctx, cf.toOptions(), *subURL, *nodesFile, mihomopool.ParseOptions{})
	if err != nil {
		return fail(err.Error(), nil)
	}
	defer stop()

	hctx, hc := ctxWithTimeout(ctx, 3*time.Minute)
	defer hc()

	h, err := pool.CheckHealth(hctx, *concurrency)
	if err != nil {
		return fail(err.Error(), nil)
	}
	detail := make([]map[string]any, 0, len(h.Detail))
	for _, d := range h.Detail {
		detail = append(detail, map[string]any{
			"node": d.Node, "delay_ms": d.DelayMS, "ok": d.OK, "error": d.Error,
		})
	}
	writeJSON(map[string]any{
		"ok": true, "kind": "health", "state": string(h.State),
		"version": h.Version, "nodes": h.Nodes, "healthy": h.Healthy, "detail": detail,
	}, *pretty)
	return 0
}

// runNodes：列出节点。
func runNodes(argv []string) int {
	fs := flag.NewFlagSet("nodes", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)
	pretty := fs.Bool("pretty", false, "缩进输出")
	refresh := fs.Bool("refresh", true, "先从控制面刷新")
	subURL := fs.String("sub", "", "订阅地址：拉取后直接起一个临时内核查看（用完即停）")
	nodesFile := fs.String("nodes", "", "节点 JSON 文件")
	keepInfo := fs.Bool("keep-info-nodes", false, "保留机场信息节点")
	keepDupes := fs.Bool("keep-duplicates", false, "保留重复节点")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, stop, err := openPool(ctx, cf.toOptions(), *subURL, *nodesFile,
		mihomopool.ParseOptions{KeepInfoNodes: *keepInfo, KeepDuplicates: *keepDupes})
	if err != nil {
		return fail(err.Error(), nil)
	}
	defer stop()
	if *refresh {
		_ = pool.RefreshNodes(ctx)
	}

	list := pool.FindNodes(mihomopool.NodeFilter{})
	writeJSON(map[string]any{
		"ok": true, "kind": "nodes", "total": len(list), "nodes": nodesToData(list),
	}, *pretty)
	return 0
}

// multiFlag 是可重复的字符串参数。
type multiFlag []string

func (m *multiFlag) String() string { return fmt.Sprint([]string(*m)) }
func (m *multiFlag) Set(v string) error {
	*m = append(*m, v)
	return nil
}

// readPIDFile 从 ready 文件里取 kernel_pid 或直接读纯数字。
func readPIDFile(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	text := trimSpaceNL(string(b))
	// 是 JSON 就取 pid
	if len(text) > 0 && text[0] == '{' {
		var v struct {
			PID       int `json:"pid"`
			KernelPID int `json:"kernel_pid"`
		}
		if err := jsonUnmarshal([]byte(text), &v); err == nil {
			if v.PID > 0 {
				return v.PID, nil
			}
			if v.KernelPID > 0 {
				return v.KernelPID, nil
			}
		}
	}
	var pid int
	if _, err := fmt.Sscanf(text, "%d", &pid); err != nil {
		return 0, fmt.Errorf("文件内容不是 PID: %q", truncateStr(text, 60))
	}
	if pid <= 0 {
		return 0, fmt.Errorf("PID 非法: %d", pid)
	}
	return pid, nil
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// jsonMarshalIndent 是 encoding/json 的薄包装，避免上层重复 import。
func jsonMarshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// jsonUnmarshal 是 encoding/json 的薄包装。
func jsonUnmarshal(b []byte, v any) error {
	return json.Unmarshal(b, v)
}

// openPool 按需决策：给了 -sub/-nodes 就自己托管一个临时内核，否则连已有内核。
//
// 这让 nodes/health 既是"查已运行内核"的工具，也能独立完成
// "给我订阅地址，直接看节点和延迟"这个最常见的诉求——
// 用户不应该为了看一眼节点先手动 start 一个内核。
func openPool(ctx context.Context, o mihomopool.Options, subURL, nodesFile string,
	po mihomopool.ParseOptions) (*mihomopool.Pool, func(), error) {

	pool, err := mihomopool.New(o)
	if err != nil {
		return nil, func() {}, err
	}

	if subURL == "" && nodesFile == "" {
		if aerr := pool.Attach(ctx); aerr != nil {
			return nil, func() {}, fmt.Errorf("连接内核失败: %w（提示：给 -sub 或 -nodes 让它自己托管内核）", aerr)
		}
		return pool, func() {}, nil
	}

	nodes, lerr := loadNodes(ctx, subURL, nodesFile, po)
	if lerr != nil {
		return nil, func() {}, fmt.Errorf("加载节点失败: %w", lerr)
	}
	if serr := pool.Start(ctx, nodes, nil); serr != nil {
		return nil, func() {}, fmt.Errorf("启动内核失败: %w", serr)
	}
	stop := func() {
		sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
		defer sc()
		_ = pool.Stop(sctx)
	}
	return pool, stop, nil
}
