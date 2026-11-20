package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// call：单次请求。适合 shell 里一条命令搞定，或 Python 里偶尔用一次。
//
// 输出格式与 serve 的响应结构一致，便于两边共用解析代码。
func runCall(argv []string) int {
	fs := flag.NewFlagSet("call", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)

	var (
		method      string
		header      string
		body        string
		bodyFile    string
		timeoutMS   int
		retries     int
		tag         string
		hashKey     string
		allowDirect bool
		skipBody    bool
		maxBody     int64
		strategy    string
		noKernel    bool
		nodesFile   string
		subURL      string
		groups      string
		types       string
		namePrefix  string
		outFile     string
		pretty      bool
	)
	fs.StringVar(&method, "method", "GET", "HTTP 方法")
	fs.StringVar(&header, "header", "", `请求头，形如 "K1: V1;K2: V2"，可重复`)
	fs.StringVar(&body, "body", "", "请求体（文本）")
	fs.StringVar(&bodyFile, "body-file", "", "从文件读请求体")
	fs.IntVar(&timeoutMS, "timeout", 20000, "超时毫秒")
	fs.IntVar(&retries, "retries", 1, "重试次数")
	fs.StringVar(&tag, "tag", "", "标签，原样带回")
	fs.StringVar(&hashKey, "hash-key", "", "一致性哈希 key")
	fs.BoolVar(&allowDirect, "allow-direct", false, "无可用节点时直连兜底")
	fs.BoolVar(&skipBody, "skip-body", false, "只取状态码")
	fs.Int64Var(&maxBody, "max-body", 1<<20, "正文上限字节")
	fs.StringVar(&strategy, "strategy", string(mihomopool.StrategyLeastInflight), "分发策略")
	fs.BoolVar(&noKernel, "no-kernel", false, "连接已有内核，不启动进程")
	fs.StringVar(&nodesFile, "nodes", "", "节点 JSON 文件")
	fs.StringVar(&subURL, "sub", "", "订阅地址")
	fs.StringVar(&groups, "groups", "", "限定分组，逗号分隔")
	fs.StringVar(&types, "types", "", "限定节点类型，逗号分隔")
	fs.StringVar(&namePrefix, "name-prefix", "", "节点名前缀")
	fs.StringVar(&outFile, "o", "", "把正文写到文件（不输出 b64）")
	fs.BoolVar(&pretty, "pretty", false, "输出带缩进的 JSON")

	if err := fs.Parse(argv); err != nil {
		return 2
	}
	url := fs.Arg(0)
	if url == "" {
		return fail("缺少 URL", map[string]any{"usage": "mihomo-pool call [参数] <url>"})
	}

	ctx, cancel := signalContext()
	defer cancel()

	pool, err := mihomopool.New(cf.toOptions())
	if err != nil {
		return fail(err.Error(), nil)
	}

	attachErr, serr := startOrAttach(ctx, pool, noKernel, subURL, nodesFile)
	if serr != nil {
		return fail(serr.Error(), nil)
	}
	defer stopPool(pool)

	if err := pool.SetStrategy(mihomopool.Strategy(strategy)); err != nil {
		return fail(err.Error(), nil)
	}
	if attachErr != nil && !allowDirect {
		return fail("连接内核失败: "+attachErr.Error(), map[string]any{
			"hint": "加 -allow-direct 可跳过内核直连；或去掉 -no-kernel 让本进程托管内核",
		})
	}

	hdrs := parseHeaderFlag(header)

	var payload []byte
	switch {
	case bodyFile != "":
		b, rerr := os.ReadFile(bodyFile)
		if rerr != nil {
			return fail("读 body 文件失败: "+rerr.Error(), nil)
		}
		payload = b
	case body != "":
		payload = []byte(body)
	}

	req := &mihomopool.Request{
		Method:       method,
		URL:          url,
		Header:       hdrs,
		Body:         payload,
		Timeout:      time.Duration(timeoutMS) * time.Millisecond,
		Retries:      retries,
		Filter:       splitFilter(groups, types, namePrefix),
		HashKey:      hashKey,
		AllowDirect:  allowDirect,
		Tag:          tag,
		SkipBody:     skipBody,
		MaxBodyBytes: maxBody,
	}

	resp := pool.Executor().Do(ctx, req)
	out := fromLibraryResponse("", resp)

	// -o 时正文落文件，JSON 里只留长度
	if outFile != "" && len(resp.Body) > 0 {
		if werr := os.WriteFile(outFile, resp.Body, 0o644); werr != nil {
			return fail("写文件失败: "+werr.Error(), nil)
		}
		out.BodyB64 = ""
		out.Data = map[string]any{"saved_to": outFile}
	}

	writeJSON(out, pretty)
	if !out.OK {
		return 1
	}
	return 0
}

// runBatch：从 stdin 或文件读请求数组，并发跑完，输出 JSON 数组。
//
// 适合不想常驻进程、但又要并发的场景（例如 crontab 里一次跑几百个）。
func runBatch(argv []string) int {
	fs := flag.NewFlagSet("batch", flag.ContinueOnError)
	var cf commonFlags
	cf.bind(fs)

	var (
		input       string
		workers     int
		perNode     int
		strategy    string
		timeout     int
		retries     int
		pretty      bool
		outFile     string
		noKernel    bool
		nodesFile   string
		subURL      string
		stream      bool
		allowDirect bool
	)
	fs.StringVar(&input, "i", "", "输入文件（默认 stdin）")
	fs.IntVar(&workers, "workers", 32, "并发数")
	fs.IntVar(&perNode, "per-node", 4, "单节点在飞上限")
	fs.StringVar(&strategy, "strategy", string(mihomopool.StrategyLeastInflight), "分发策略")
	fs.IntVar(&timeout, "timeout", 20000, "默认超时毫秒")
	fs.IntVar(&retries, "retries", 1, "默认重试次数")
	fs.BoolVar(&pretty, "pretty", false, "输出缩进 JSON")
	fs.StringVar(&outFile, "o", "", "输出到文件（默认 stdout）")
	fs.BoolVar(&noKernel, "no-kernel", false, "连接已有内核")
	fs.StringVar(&nodesFile, "nodes", "", "节点 JSON 文件")
	fs.StringVar(&subURL, "sub", "", "订阅地址")
	fs.BoolVar(&stream, "stream", false, "边完成边输出 JSONL（不攒成数组）")
	fs.BoolVar(&allowDirect, "allow-direct", false, "默认让所有请求允许直连兜底")

	if err := fs.Parse(argv); err != nil {
		return 2
	}

	ctx, cancel := signalContext()
	defer cancel()

	var in io.Reader = os.Stdin
	if input != "" {
		f, err := os.Open(input)
		if err != nil {
			return fail("打开输入失败: "+err.Error(), nil)
		}
		defer f.Close()
		in = f
	}

	msgs, err := readBatchInput(in, timeout, retries)
	if err != nil {
		return fail(err.Error(), nil)
	}
	if allowDirect {
		for _, m := range msgs {
			if m.AllowDirect == nil {
				v := true
				m.AllowDirect = &v
			}
		}
	}
	if len(msgs) == 0 {
		return fail("输入里没有请求", nil)
	}

	pool, err := mihomopool.New(cf.toOptions())
	if err != nil {
		return fail(err.Error(), nil)
	}
	if _, serr := startOrAttach(ctx, pool, noKernel, subURL, nodesFile); serr != nil {
		return fail(serr.Error(), nil)
	}
	defer stopPool(pool)

	if err := pool.SetStrategy(mihomopool.Strategy(strategy)); err != nil {
		return fail(err.Error(), nil)
	}
	pool.ConfigureExecutor(workers, perNode)

	var outf *os.File
	if outFile != "" {
		f, err := os.Create(outFile)
		if err != nil {
			return fail("创建输出失败: "+err.Error(), nil)
		}
		defer f.Close()
		outf = f
	} else {
		outf = os.Stdout
	}

	if stream {
		return runBatchStream(ctx, pool, msgs, outf)
	}
	return runBatchCollect(ctx, pool, msgs, outf, pretty)
}

// batchKey 是批量执行时占据 Request.Tag 的内部标记。
//
// 为什么借用 Tag：Response.Tag 是唯一能原样穿过执行器的字段，
// 而 ForEach 的回调顺序不保证，不能靠位置索引对号入座。
// 业务自己的 tag 被搬到 Origin 里，回包时再还原。
type batchKey struct {
	ID     string
	Origin string
}

// prepareBatch 把协议消息转成库请求，并把 id 编进 Tag。
func prepareBatch(msgs []*requestMsg) (reqs []*mihomopool.Request, origin map[string]string, errs map[int]*responseMsg) {
	errs = map[int]*responseMsg{}
	origin = map[string]string{}
	for i, m := range msgs {
		lreq, err := m.toLibraryRequest()
		if err != nil {
			errs[i] = &responseMsg{ID: m.ID, Op: "request", Kind: "err", OK: false, Error: err.Error()}
			continue
		}
		origin[m.ID] = lreq.Tag
		lreq.Tag = m.ID
		reqs = append(reqs, lreq)
	}
	return reqs, origin, errs
}

// responseFor 把执行器结果还原成协议消息。
func responseFor(resp *mihomopool.Response, origin map[string]string) *responseMsg {
	id := resp.Tag
	out := fromLibraryResponse(id, resp)
	out.Tag = origin[id]
	return out
}

// runBatchCollect 收集全部结果后一次性输出数组。
//
// 必须走 pool.Executor().ForEach 而不是自己起 goroutine：
// 执行器内部的 worker 池与"每节点在飞上限"是一套约束，绕过它直接
// 并发调用 Do 会瞬间打满上限，结果是大量"在飞已满"的假失败。
func runBatchCollect(ctx context.Context, pool *mihomopool.Pool, msgs []*requestMsg, out *os.File, pretty bool) int {
	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()

	results := make([]*responseMsg, len(msgs))
	reqs, origin, errs := prepareBatch(msgs)
	for i, e := range errs {
		results[i] = e
	}

	// 按下标记录每个已完成请求对应的原始位置
	posOf := make(map[string]int, len(msgs))
	for i, m := range msgs {
		posOf[m.ID] = i
	}

	var mu sync.Mutex
	_ = pool.Executor().ForEach(ctx, reqs, func(resp *mihomopool.Response) {
		out := responseFor(resp, origin)
		mu.Lock()
		if idx, ok := posOf[out.ID]; ok {
			results[idx] = out
		}
		mu.Unlock()
	})

	var data []byte
	var err error
	if pretty {
		data, err = json.MarshalIndent(results, "", "  ")
	} else {
		data, err = json.Marshal(results)
	}
	if err != nil {
		return fail("序列化结果失败: "+err.Error(), nil)
	}
	w.Write(data)
	w.WriteByte('\n')
	_ = w.Flush()

	failed := 0
	for _, r := range results {
		if r != nil && !r.OK {
			failed++
		}
	}
	if failed > 0 {
		return 1
	}
	return 0
}

// runBatchStream 边完成边输出 JSONL。
//
// 同样走执行器的 worker 池：并发度由 -workers 与 -per-node 共同决定，
// 流式输出只是把结果即时写出去，不改变调度方式。
func runBatchStream(ctx context.Context, pool *mihomopool.Pool, msgs []*requestMsg, out *os.File) int {
	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()

	failed := 0
	writeOne := func(r *responseMsg) {
		b, _ := json.Marshal(r)
		w.Write(b)
		w.WriteByte('\n')
		_ = w.Flush() // 流式要立即刷
		if !r.OK {
			failed++
		}
	}

	reqs, origin, errs := prepareBatch(msgs)
	for _, e := range errs {
		writeOne(e)
	}

	var mu sync.Mutex
	_ = pool.Executor().ForEach(ctx, reqs, func(resp *mihomopool.Response) {
		out := responseFor(resp, origin)
		mu.Lock()
		writeOne(out)
		mu.Unlock()
	})

	if failed > 0 {
		return 1
	}
	return 0
}

// readBatchInput 解析批量输入，支持三种形态：
//   - JSON 数组：[{...}, {...}]
//   - JSONL：每行一个对象
//   - 纯 URL 行：每行一个 URL
//
// 注意：stdin 不可 Seek，所以这里只读一次、在内存里判定格式，
// 不能在读完后再回头 Scan（管道上 Seek 会直接 EINVAL）。
func readBatchInput(r io.Reader, defTimeout, defRetries int) ([]*requestMsg, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("读输入失败: %w", err)
	}
	trimmed := trimSpaceNL(string(b))
	if trimmed == "" {
		return nil, nil
	}

	if trimmed[0] == '[' {
		var arr []requestMsg
		if err := json.Unmarshal([]byte(trimmed), &arr); err != nil {
			return nil, fmt.Errorf("解析 JSON 数组失败: %w", err)
		}
		return applyBatchDefaults(arr, defTimeout, defRetries), nil
	}
	return scanBatch(strings.NewReader(trimmed), defTimeout, defRetries)
}

// scanBatch 逐行解析。
func scanBatch(r io.Reader, defTimeout, defRetries int) ([]*requestMsg, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 16<<20)

	var out []requestMsg
	for sc.Scan() {
		line := trimSpaceNL(sc.Text())
		if line == "" {
			continue
		}
		if line[0] == '{' {
			var m requestMsg
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				return nil, fmt.Errorf("第 %d 行解析失败: %w", len(out)+1, err)
			}
			out = append(out, m)
			continue
		}
		// 纯 URL 行
		out = append(out, requestMsg{URL: line, Op: "request"})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return applyBatchDefaults(out, defTimeout, defRetries), nil
}

// applyBatchDefaults 给缺省字段补默认值。
//
// 关键：id 是调度侧的唯一键。批量输入里常常不带 id，
// 若不给它生成一个，所有请求的 id 都是空串，回填结果时会互相覆盖，
// 最终数组里只剩一条被反复重写。这里统一补成内部序号。
func applyBatchDefaults(in []requestMsg, defTimeout, defRetries int) []*requestMsg {
	out := make([]*requestMsg, 0, len(in))
	seen := map[string]int{}
	for i := range in {
		m := in[i]
		if m.Op == "" {
			m.Op = "request"
		}
		if m.TimeoutMS <= 0 {
			m.TimeoutMS = defTimeout
		}
		if m.Retries == nil {
			r := defRetries
			m.Retries = &r
		}
		if m.ID == "" {
			m.ID = fmt.Sprintf("#%d", i)
		}
		// 用户自己给了重复 id 也要消歧，否则同样会互相覆盖
		if n, dup := seen[m.ID]; dup {
			seen[m.ID] = n + 1
			m.ID = fmt.Sprintf("%s~%d", m.ID, n+1)
		} else {
			seen[m.ID] = 1
		}
		out = append(out, &m)
	}
	return out
}

// startOrAttach 按 noKernel 决定启动内核还是连接已有内核。
//
// noKernel 时如果控制面连不上，不直接报错：调用方可能就是想用
// -allow-direct 直连跑一次（例如临时抓个页面），这时应该放行，
// 让执行器把"无可用节点"交给 AllowDirect 分支处理。
func startOrAttach(ctx context.Context, pool *mihomopool.Pool, noKernel bool, subURL, nodesFile string) (attachErr error, err error) {
	if noKernel {
		if aerr := pool.Attach(ctx); aerr != nil {
			return aerr, nil
		}
		return nil, nil
	}
	nodes, lerr := loadNodes(ctx, subURL, nodesFile, mihomopool.ParseOptions{})
	if lerr != nil {
		return nil, fmt.Errorf("加载节点失败: %w", lerr)
	}
	return nil, pool.Start(ctx, nodes, nil)
}

// stopPool 收尾。
func stopPool(pool *mihomopool.Pool) {
	sctx, sc := context.WithTimeout(context.Background(), 15*time.Second)
	defer sc()
	_ = pool.Stop(sctx)
}

// parseHeaderFlag 解析 "K1: V1;K2: V2"。
func parseHeaderFlag(s string) map[string]string {
	out := map[string]string{}
	s = trimSpaceNL(s)
	if s == "" {
		return out
	}
	for _, part := range splitOn(s, ';', '\n') {
		part = trimSpaceNL(part)
		if part == "" {
			continue
		}
		idx := indexByte(part, ':')
		if idx <= 0 {
			continue
		}
		k := trimSpaceNL(part[:idx])
		v := trimSpaceNL(part[idx+1:])
		if k != "" {
			out[k] = v
		}
	}
	return out
}

// splitFilter 把逗号分隔的筛选参数转成 NodeFilter。
func splitFilter(groups, types, prefix string) mihomopool.NodeFilter {
	return mihomopool.NodeFilter{
		Groups:     splitCSVFlag(groups),
		Types:      splitCSVFlag(types),
		NamePrefix: prefix,
	}
}

// splitCSVFlag 按逗号切分并去空。
func splitCSVFlag(s string) []string {
	s = trimSpaceNL(s)
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range splitOn(s, ',') {
		if p = trimSpaceNL(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// splitOn 按任一分隔符切分。
func splitOn(s string, seps ...byte) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		for _, sep := range seps {
			if s[i] == sep {
				out = append(out, s[start:i])
				start = i + 1
				break
			}
		}
	}
	out = append(out, s[start:])
	return out
}

// writeJSON 输出一条 JSON。
func writeJSON(v any, pretty bool) {
	var b []byte
	var err error
	if pretty {
		b, err = json.MarshalIndent(v, "", "  ")
	} else {
		b, err = json.Marshal(v)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "mihomo-pool: 序列化失败: %v\n", err)
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

// trimSpaceNL 去首尾空白。
func trimSpaceNL(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}
