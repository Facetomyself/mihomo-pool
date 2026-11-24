package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// 订阅池子命令：多个订阅的统一编排、健康熔断、权重配额与标签筛选。

// splitSubAction 从参数里取出动作与全局参数、其余参数。
//
// 返回：
//   - action：第一个裸词（list/refresh/nodes/check）
//   - rest：剩下要交给子命令 FlagSet 的参数（已剔除动作）
//   - globals：全局参数（-bin 等）及其值，由调用方手动喂给公共 FlagSet
//
// 全局参数不能留在 rest 里：子命令的 FlagSet 只注册了自己的参数，
// 遇到 -bin 会直接报 "flag provided but not defined"。
func splitSubAction(argv []string) (action string, rest, globals []string) {
	rest = make([]string, 0, len(argv))
	i := 0
	for i < len(argv) {
		a := argv[i]
		if strings.HasPrefix(a, "-") {
			name := strings.TrimLeft(a, "-")
			hasVal := strings.IndexByte(name, '=') < 0
			if idx := strings.IndexByte(name, '='); idx >= 0 {
				name = name[:idx]
			}
			if globalFlagNames[name] {
				globals = append(globals, a)
				if hasVal && name != "allow-lan" && i+1 < len(argv) {
					globals = append(globals, argv[i+1])
					i += 2
					continue
				}
				i++
				continue
			}
			rest = append(rest, a)
			// 子参数也可能带值（-n 10）
			if hasVal && i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "-") {
				rest = append(rest, argv[i+1])
				i += 2
				continue
			}
			i++
			continue
		}
		if action == "" {
			action = a
		} else {
			rest = append(rest, a)
		}
		i++
	}
	return action, rest, globals
}

// subSpecFlag 是可重复的订阅定义：name=url[|interval][,quota=0.4][,weight=2]
type subSpecFlag struct {
	specs []mihomopool.SubscriptionSpec
}

// Set 解析一条订阅定义。
//
// 格式（[] 表示可选，用逗号分隔属性）：
//
//	name=url
//	name=url|30m
//	name=url|30m,quota=0.4,weight=2,priority=3
//
// 也接受裸 URL（自动命名 sub1/sub2…）。
func (s *subSpecFlag) String() string {
	parts := make([]string, 0, len(s.specs))
	for _, sp := range s.specs {
		parts = append(parts, sp.Name+"="+sp.URL)
	}
	return strings.Join(parts, ",")
}

func (s *subSpecFlag) Set(v string) error {
	spec, err := parseSubSpec(v, len(s.specs)+1)
	if err != nil {
		return err
	}
	s.specs = append(s.specs, spec)
	return nil
}

// parseSubSpec 解析单条订阅定义。
func parseSubSpec(raw string, idx int) (mihomopool.SubscriptionSpec, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return mihomopool.SubscriptionSpec{}, fmt.Errorf("订阅定义为空")
	}

	spec := mihomopool.SubscriptionSpec{}
	// 先切 name=url 与属性
	main := v
	attrs := ""
	if i := strings.Index(v, ","); i > 0 {
		main = v[:i]
		attrs = v[i+1:]
	}

	if i := strings.Index(main, "="); i > 0 && !strings.Contains(main[:i], "/") {
		spec.Name = strings.TrimSpace(main[:i])
		main = strings.TrimSpace(main[i+1:])
	} else {
		spec.Name = fmt.Sprintf("sub%d", idx)
	}

	// interval 用 | 跟在 URL 后
	if i := strings.Index(main, "|"); i > 0 {
		d, err := time.ParseDuration(strings.TrimSpace(main[i+1:]))
		if err == nil {
			spec.Interval = d
		}
		main = strings.TrimSpace(main[:i])
	}
	spec.URL = main
	if spec.URL == "" {
		return mihomopool.SubscriptionSpec{}, fmt.Errorf("订阅 %s 缺少 URL", spec.Name)
	}

	for _, kv := range strings.Split(attrs, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) != 2 {
			// 裸标签当作订阅级 tag
			spec.Tags = append(spec.Tags, kv)
			continue
		}
		key := strings.ToLower(strings.TrimSpace(parts[0]))
		val := strings.TrimSpace(parts[1])
		switch key {
		case "quota":
			var f float64
			if _, err := fmt.Sscanf(val, "%g", &f); err != nil {
				return mihomopool.SubscriptionSpec{}, fmt.Errorf("quota 解析失败: %q", val)
			}
			spec.Quota = f
		case "weight":
			var f float64
			if _, err := fmt.Sscanf(val, "%g", &f); err != nil {
				return mihomopool.SubscriptionSpec{}, fmt.Errorf("weight 解析失败: %q", val)
			}
			spec.Weight = f
		case "priority", "prio":
			var n int
			if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
				return mihomopool.SubscriptionSpec{}, fmt.Errorf("priority 解析失败: %q", val)
			}
			spec.Priority = n
		case "tag":
			spec.Tags = append(spec.Tags, val)
		case "header":
			if spec.Header == nil {
				spec.Header = map[string]string{}
			}
			hp := strings.SplitN(val, ":", 2)
			if len(hp) == 2 {
				spec.Header[strings.TrimSpace(hp[0])] = strings.TrimSpace(hp[1])
			}
		default:
			return mihomopool.SubscriptionSpec{}, fmt.Errorf("未知属性 %q（可用 quota/weight/priority/tag/header）", key)
		}
	}
	return spec, nil
}

// subpoolFlags 是订阅池子命令共用的参数。
type subpoolFlags struct {
	subs      subSpecFlag
	interval  time.Duration
	maxFail   int
	halfOpen  time.Duration
	dedupe    bool
	userAgent string
}

func (f *subpoolFlags) bind(fs *flag.FlagSet) {
	fs.Var(&f.subs, "sub",
		"订阅定义，可重复。格式 name=url[|interval][,quota=0.4][,weight=2][,priority=1][,tag=xxx]")
	fs.DurationVar(&f.interval, "interval", time.Hour, "默认刷新周期")
	fs.IntVar(&f.maxFail, "max-fail", 3, "连续失败几次后熔断该订阅")
	fs.DurationVar(&f.halfOpen, "half-open", 5*time.Minute, "熔断后多久尝试恢复")
	fs.BoolVar(&f.dedupe, "dedupe", false,
		"跨订阅按连接指纹去重（默认关闭：不同订阅的额度独立，合并会丢失归属）")
	fs.StringVar(&f.userAgent, "ua", "", "拉订阅用的 User-Agent（默认 clash-verge/2.0）")
}

func (f *subpoolFlags) build() (*mihomopool.SubscriptionPool, error) {
	if len(f.subs.specs) == 0 {
		return nil, fmt.Errorf("至少给一个 -sub")
	}
	opts := mihomopool.SubscriptionPoolOptions{
		DefaultInterval:    f.interval,
		MaxConsecutiveFail: f.maxFail,
		CircuitHalfOpen:    f.halfOpen,
		DedupeAcrossSubs:   f.dedupe,
	}
	if f.userAgent != "" {
		opts.Fetch.UserAgent = f.userAgent
	}
	pool := mihomopool.NewSubscriptionPool(opts)
	if err := pool.AddMany(f.subs.specs); err != nil {
		return nil, err
	}
	return pool, nil
}

// runSubpool 是 `subpool` 子命令：管理多订阅。
//
// 参数顺序兼容三种写法：
//
//	mihomo-pool subpool check -n 10 ...
//	mihomo-pool -bin X subpool check -n 10 ...
//	mihomo-pool subpool -bin X check -n 10 ...
//
// 后两种会把全局参数插到动作前面，所以必须先剥离它们再取动作。
func runSubpool(argv []string) int {
	action, rest, globals := splitSubAction(argv)
	if action == "" {
		return fail("需要子动作: list | refresh | nodes | check", nil)
	}
	// 全局参数重新拼回 rest 尾部：各子命令都注册了 commonFlags，
	// 能解析它们；拼回尾部保证不影响子参数的取值。
	argv = append(rest, globals...)

	switch action {
	case "list":
		return subpoolList(argv)
	case "refresh":
		return subpoolRefresh(argv)
	case "nodes":
		return subpoolNodes(argv)
	case "check":
		return subpoolCheck(argv)
	default:
		return fail("未知动作: "+action+"（可用 list/refresh/nodes/check）", nil)
	}
}

// loadSubsFile 从 JSON 文件读订阅定义，便于订阅多时用文件维护。
func loadSubsFile(path string) ([]mihomopool.SubscriptionSpec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw []struct {
		Name     string            `json:"name"`
		URL      string            `json:"url"`
		Interval string            `json:"interval"`
		Priority int               `json:"priority"`
		Weight   float64           `json:"weight"`
		Quota    float64           `json:"quota"`
		Tags     []string          `json:"tags"`
		Disabled bool              `json:"disabled"`
		Header   map[string]string `json:"header"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, fmt.Errorf("解析订阅文件失败: %w", err)
	}
	out := make([]mihomopool.SubscriptionSpec, 0, len(raw))
	for _, r := range raw {
		spec := mihomopool.SubscriptionSpec{
			Name: r.Name, URL: r.URL, Priority: r.Priority,
			Weight: r.Weight, Quota: r.Quota, Tags: r.Tags,
			Disabled: r.Disabled, Header: r.Header,
		}
		if r.Interval != "" {
			if d, err := time.ParseDuration(r.Interval); err == nil {
				spec.Interval = d
			}
		}
		out = append(out, spec)
	}
	return out, nil
}

// subpoolWithFile 构造订阅池，支持 -file 或 -sub。
func subpoolWithFile(f *subpoolFlags, file string) (*mihomopool.SubscriptionPool, error) {
	if file != "" {
		specs, err := loadSubsFile(file)
		if err != nil {
			return nil, err
		}
		opts := mihomopool.SubscriptionPoolOptions{
			DefaultInterval:    f.interval,
			MaxConsecutiveFail: f.maxFail,
			CircuitHalfOpen:    f.halfOpen,
			DedupeAcrossSubs:   f.dedupe,
		}
		if f.userAgent != "" {
			opts.Fetch.UserAgent = f.userAgent
		}
		pool := mihomopool.NewSubscriptionPool(opts)
		if err := pool.AddMany(specs); err != nil {
			return nil, err
		}
		return pool, nil
	}
	return f.build()
}

// subpoolList 列出订阅状态。
func subpoolList(argv []string) int {
	fs := flag.NewFlagSet("subpool list", flag.ContinueOnError)
	var f subpoolFlags
	f.bind(fs)
	// list 不启动内核，commonFlags 只为了让全局参数语法可用
	var cf commonFlags
	cf.bind(fs)
	file := fs.String("file", "", "从 JSON 文件读订阅定义")
	refresh := fs.Bool("refresh", false, "先刷新再列出")
	pretty := fs.Bool("pretty", false, "缩进输出")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	pool, err := subpoolWithFile(&f, *file)
	if err != nil {
		return fail(err.Error(), nil)
	}

	ctx := context.Background()
	if *refresh {
		pool.RefreshAll(ctx, true)
	}
	infos := pool.Infos()
	mihomopool.SortInfosByWeight(infos)

	out := make([]map[string]any, 0, len(infos))
	for _, in := range infos {
		out = append(out, map[string]any{
			"name": in.Name, "url": in.URL, "state": string(in.State),
			"nodes": in.Nodes, "healthy": in.Healthy,
			"priority": in.Priority, "weight": in.Weight, "quota": in.Quota,
			"eff_weight": in.EffWeight,
			"refresh_ok": in.RefreshOK, "refresh_fail": in.RefreshFail,
			"consec_fail":  in.ConsecFail,
			"last_refresh": in.LastRefresh.Format(time.RFC3339),
			"next_refresh": in.NextRefresh.Format(time.RFC3339),
			"last_error":   in.LastError,
		})
	}
	writeJSON(map[string]any{"ok": true, "kind": "subpool", "total": len(out), "subs": out}, *pretty)
	return 0
}

// subpoolRefresh 拉取所有订阅。
func subpoolRefresh(argv []string) int {
	fs := flag.NewFlagSet("subpool refresh", flag.ContinueOnError)
	var f subpoolFlags
	f.bind(fs)
	// 同上：不启动内核，只为接受全局参数语法
	var cf commonFlags
	cf.bind(fs)
	file := fs.String("file", "", "从 JSON 文件读订阅定义")
	pretty := fs.Bool("pretty", false, "缩进输出")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	pool, err := subpoolWithFile(&f, *file)
	if err != nil {
		return fail(err.Error(), nil)
	}

	ctx, cancel := signalContext()
	defer cancel()
	ok, bad := pool.RefreshAll(ctx, true)

	nodes, _ := pool.Nodes()
	writeJSON(map[string]any{
		"ok": bad == 0, "kind": "refresh",
		"updated": ok, "failed": bad, "nodes": len(nodes),
	}, *pretty)
	if bad > 0 {
		return 1
	}
	return 0
}

// subpoolNodes 列出订阅池里的节点（可带标签筛选）。
func subpoolNodes(argv []string) int {
	fs := flag.NewFlagSet("subpool nodes", flag.ContinueOnError)
	var f subpoolFlags
	f.bind(fs)
	// 同上：不启动内核，只为接受全局参数语法
	var cf commonFlags
	cf.bind(fs)
	file := fs.String("file", "", "从 JSON 文件读订阅定义")
	region := fs.String("region", "", "只要某地区（HK/JP/US…），逗号分隔")
	rate := fs.String("rate", "", "只要某倍率（0.1 表示 ≤0.1），-1 表示只要不限速")
	tags := fs.String("tag", "", "只要包含这些标签，逗号分隔")
	proto := fs.String("proto", "", "只要某协议，逗号分隔")
	limit := fs.Int("limit", 0, "最多输出多少条（0=不限）")
	pretty := fs.Bool("pretty", false, "缩进输出")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	pool, err := subpoolWithFile(&f, *file)
	if err != nil {
		return fail(err.Error(), nil)
	}
	ctx, cancel := signalContext()
	defer cancel()
	pool.RefreshAll(ctx, true)

	nodes, _ := pool.Nodes()
	filtered := filterNodesByTags(nodes, *region, *rate, *tags, *proto)
	if *limit > 0 && len(filtered) > *limit {
		filtered = filtered[:*limit]
	}

	items := make([]map[string]any, 0, len(filtered))
	for _, n := range filtered {
		items = append(items, map[string]any{
			"name": n.Name, "type": n.Type, "sub": n.Subscription,
			"server": n.Server, "port": n.Port, "tags": n.Tags,
		})
	}
	writeJSON(map[string]any{
		"ok": true, "kind": "nodes",
		"total": len(nodes), "matched": len(items), "nodes": items,
	}, *pretty)
	return 0
}

// filterNodesByTags 按标签筛选节点。
//
// rate 语义：给定 0.1 表示只要倍率 ≤ 0.1；给 -1 表示只要不限速（无 rate 标签）。
func filterNodesByTags(nodes []mihomopool.NodeSpec, region, rate, tags, proto string) []mihomopool.NodeSpec {
	regions := splitCSVFlag(region)
	protos := splitCSVFlag(proto)
	wantTags := splitCSVFlag(tags)
	wantRate := -999.0
	if rate != "" {
		var v float64
		if _, err := fmt.Sscanf(rate, "%g", &v); err == nil {
			wantRate = v
		}
	}

	out := make([]mihomopool.NodeSpec, 0, len(nodes))
	for _, n := range nodes {
		if len(regions) > 0 {
			ok := false
			for _, r := range regions {
				if mihomopool.HasTag(n.Tags, "region:"+strings.ToUpper(r)) {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		if len(protos) > 0 {
			ok := false
			for _, p := range protos {
				if mihomopool.HasTag(n.Tags, "proto:"+strings.ToLower(p)) {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		if wantRate != -999 {
			has := false
			matched := false
			for _, t := range n.Tags {
				if strings.HasPrefix(t, "rate:") {
					has = true
					var v float64
					if _, err := fmt.Sscanf(t[5:], "%g", &v); err == nil {
						if wantRate < 0 {
							// 只要不限速
						} else if v <= wantRate {
							matched = true
						}
					}
				}
			}
			if wantRate < 0 {
				if has {
					continue // 有倍率标记说明不是不限速
				}
			} else if !matched {
				continue
			}
		}
		allTags := true
		for _, wt := range wantTags {
			if !mihomopool.HasTag(n.Tags, wt) {
				allTags = false
				break
			}
		}
		if !allTags {
			continue
		}
		out = append(out, n)
	}
	return out
}

// subpoolCheck 跑一次真实请求验证订阅池可用性。
func subpoolCheck(argv []string) int {
	fs := flag.NewFlagSet("subpool check", flag.ContinueOnError)
	var f subpoolFlags
	f.bind(fs)
	var cf commonFlags
	cf.bind(fs)
	file := fs.String("file", "", "从 JSON 文件读订阅定义")
	target := fs.String("target", "https://www.gstatic.com/generate_204", "探测目标")
	count := fs.Int("n", 10, "请求数")
	workers := fs.Int("workers", 8, "并发数")
	pretty := fs.Bool("pretty", false, "缩进输出")
	if err := fs.Parse(argv); err != nil {
		return 2
	}

	pool, err := subpoolWithFile(&f, *file)
	if err != nil {
		return fail(err.Error(), nil)
	}

	ctx, cancel := signalContext()
	defer cancel()

	ok, bad := pool.RefreshAll(ctx, true)
	if ok == 0 {
		return fail("所有订阅都刷新失败", map[string]any{"failed": bad})
	}

	o := cf.toOptions()
	np, err := mihomopool.New(o)
	if err != nil {
		return fail(err.Error(), nil)
	}
	if err := pool.ApplyTo(ctx, np); err != nil {
		return fail("内核启动失败: "+err.Error(), nil)
	}
	defer func() {
		sctx, sc := context.WithTimeout(context.Background(), 20*time.Second)
		defer sc()
		_ = np.Stop(sctx)
	}()

	np.ConfigureExecutor(*workers, 4)
	reqs := make([]*mihomopool.Request, 0, *count)
	for i := 0; i < *count; i++ {
		reqs = append(reqs, &mihomopool.Request{
			URL: *target, Timeout: 20 * time.Second, Retries: 1,
		})
	}

	var success int
	byNode := map[string]int{}
	_ = np.Executor().ForEach(ctx, reqs, func(r *mihomopool.Response) {
		if r.OK() {
			success++
			byNode[r.Node]++
		}
	})

	pool.SyncHealth(np)
	infos := pool.Infos()
	subs := make([]map[string]any, 0, len(infos))
	for _, in := range infos {
		subs = append(subs, map[string]any{
			"name": in.Name, "state": string(in.State),
			"nodes": in.Nodes, "healthy": in.Healthy, "eff_weight": in.EffWeight,
		})
	}

	writeJSON(map[string]any{
		"ok": success > 0, "kind": "check",
		"requests": *count, "success": success,
		"node_hits": byNode, "subs": subs,
		"quota": np.QuotaSnapshot(),
	}, *pretty)
	if success == 0 {
		return 1
	}
	return 0
}
