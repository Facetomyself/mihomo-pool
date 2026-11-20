package mihomopool

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// 配置生成。
//
// 与"把订阅 YAML 原文拼到配置末尾"的做法不同，这里只生成骨架：
// 订阅以 proxy-providers 形式交给内核自己定时拉取，因此订阅里的
// mixed-port / listeners / external-controller 永远不会覆盖我们的设置。
//
// 每个节点对应一条 listeners 条目，listeners[n].proxy 绑定到指定出站，
// 这样 N 个节点就是 N 个独立本地端口，互不干扰。

// NodeSpec 描述一个要暴露成本地端口的出站节点。
type NodeSpec struct {
	// Name 是节点在 mihomo 里的名字，也是 selector/proxy 引用它的键。
	// 必须唯一且非空。
	Name string
	// Type 是 mihomo 节点类型，如 ss / vmess / trojan / http / socks5 / hysteria2 / tuic。
	Type string
	// Server/Port 是远端地址（仅用于展示与兜底构造；完整参数走 Raw）。
	Server string
	Port   int
	// Raw 是节点的原始字段集合（来自订阅解析），会被原样写进 proxies。
	// Raw 里已含 name/type 时以 Name/Type 字段为准。
	Raw map[string]any
	// Group 是节点分组标签（如 "hk"、"us"），仅用于业务侧分发，不影响配置。
	Group string
}

// ProviderSpec 描述一个交给内核自拉取的机场订阅。
type ProviderSpec struct {
	// Name 是 provider 键名（proxy-providers 下的键），必须唯一。
	Name string
	// URL 是订阅地址。
	URL string
	// IntervalSec 是内核刷新间隔（秒），为 0 时不写入 interval。
	IntervalSec int
	// HealthCheckURL 是 provider 健康检查地址，为空用默认。
	HealthCheckURL string
	// HealthCheckIntervalSec 是健康检查间隔（秒），为 0 用默认。
	HealthCheckIntervalSec int
}

// listenerConfig 是 mihomo listeners 条目。
type listenerConfig struct {
	Name   string `yaml:"name"`
	Type   string `yaml:"type"`
	Port   int    `yaml:"port"`
	Listen string `yaml:"listen"`
	Proxy  string `yaml:"proxy"`
	UDP    bool   `yaml:"udp,omitempty"`
}

// listenerBinding 记录节点到本地端口的映射。
type listenerBinding struct {
	Node     string
	Port     int
	ProxyURL string
}

// renderConfig 生成完整的 mihomo 配置文本。
//
// 节点来自两种来源：
//   - inline 节点（NodeSpec）直接写进 proxies
//   - provider 订阅写进 proxy-providers，节点名运行时才有
//
// 只有 inline 节点能预分配固定端口。provider 节点在 reload 拿到节点名后
// 由 EnsureProviderListeners 二次分配，见 listener 段的重写逻辑。
func (o *Options) renderConfig(nodes []NodeSpec, providers []ProviderSpec) (string, []listenerBinding, error) {
	seen := map[string]bool{}
	proxies := make([]map[string]any, 0, len(nodes))
	bindings := make([]listenerBinding, 0, len(nodes))

	for i, n := range nodes {
		name := strings.TrimSpace(n.Name)
		if name == "" {
			return "", nil, fmt.Errorf("mihomo-pool: 第 %d 个节点缺少 Name", i)
		}
		if seen[name] {
			return "", nil, fmt.Errorf("mihomo-pool: 节点名重复: %s", name)
		}
		seen[name] = true

		entry := map[string]any{}
		for k, v := range n.Raw {
			entry[k] = v
		}
		entry["name"] = name
		if t := strings.TrimSpace(n.Type); t != "" {
			entry["type"] = t
		} else if _, ok := entry["type"]; !ok {
			return "", nil, fmt.Errorf("mihomo-pool: 节点 %s 缺少 Type", name)
		}
		if n.Server != "" {
			if _, ok := entry["server"]; !ok {
				entry["server"] = n.Server
			}
		}
		if n.Port > 0 {
			if _, ok := entry["port"]; !ok {
				entry["port"] = n.Port
			}
		}
		proxies = append(proxies, entry)

		port := o.BasePort + i
		bindings = append(bindings, listenerBinding{
			Node:     name,
			Port:     port,
			ProxyURL: o.localProxyURL(port),
		})
	}

	var b yaml.Node // 用 Node 构建，保证 key 顺序稳定、空段不出现
	root := &b
	root.Kind = yaml.MappingNode

	addScalar := func(key string, val any) {
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: key},
			mustScalar(val),
		)
	}

	addScalar("mixed-port", o.SocksPort)
	addScalar("allow-lan", o.AllowLAN)
	addScalar("mode", "rule")
	addScalar("log-level", o.LogLevel)
	if o.Secret != "" {
		addScalar("secret", o.Secret)
	}

	apiAddr := o.APIHost + ":" + strconv.Itoa(o.APIPort)
	addScalar("external-controller", apiAddr)

	// DNS 兜底：机场订阅常带 DNS 段，这里给一份能用的默认，避免节点域名解析失败。
	root.Content = append(root.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: "dns"},
		buildDNSNode(),
	)

	if len(proxies) > 0 {
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "proxies"},
			mustMarshalNode(proxies),
		)
	}

	if len(providers) > 0 {
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "proxy-providers"},
			buildProvidersNode(providers),
		)
	}

	if len(bindings) > 0 {
		ls := make([]listenerConfig, 0, len(bindings))
		for _, bd := range bindings {
			ls = append(ls, listenerConfig{
				Name:   listenerName(bd.Node),
				Type:   string(o.Mode),
				Port:   bd.Port,
				Listen: listenAddr(o.AllowLAN),
				Proxy:  bd.Node,
			})
		}
		root.Content = append(root.Content,
			&yaml.Node{Kind: yaml.ScalarNode, Value: "listeners"},
			mustMarshalNode(ls),
		)
	}

	out, err := yaml.Marshal(root)
	if err != nil {
		return "", nil, fmt.Errorf("mihomo-pool: 生成配置失败: %w", err)
	}

	header := "# 由 mihomo-pool 生成，请勿手改；改 Options 后重新生成。\n"
	return header + string(out), bindings, nil
}

// buildDNSNode 给一份保守可用的 DNS 配置。
func buildDNSNode() *yaml.Node {
	m := map[string]any{
		"enable":             true,
		"ipv6":               false,
		"enhanced-mode":      "fake-ip",
		"fake-ip-range":      "198.18.0.1/16",
		"default-nameserver": []string{"223.5.5.5", "119.29.29.29"},
		"nameserver":         []string{"https://doh.pub/dns-query", "https://dns.alidns.com/dns-query"},
		"fallback":           []string{"https://dns.google/dns-query", "https://cloudflare-dns.com/dns-query"},
	}
	return mustMarshalNode(m)
}

// buildProvidersNode 构造 proxy-providers 段。
func buildProvidersNode(providers []ProviderSpec) *yaml.Node {
	out := map[string]any{}
	seen := map[string]bool{}
	for _, p := range providers {
		name := strings.TrimSpace(p.Name)
		if name == "" || strings.TrimSpace(p.URL) == "" {
			continue
		}
		if seen[name] {
			continue
		}
		seen[name] = true

		body := map[string]any{
			"type": "http",
			"url":  p.URL,
			"path": "./providers/" + sanitizeFileName(name) + ".yaml",
			"health-check": map[string]any{
				"enable":   true,
				"url":      firstNonEmpty(p.HealthCheckURL, "https://www.gstatic.com/generate_204"),
				"interval": p.HealthCheckIntervalSec,
			},
		}
		if body["health-check"].(map[string]any)["interval"] == 0 {
			body["health-check"].(map[string]any)["interval"] = 300
		}
		if p.IntervalSec > 0 {
			body["interval"] = p.IntervalSec
		}
		out[name] = body
	}
	keys := make([]string, 0, len(out))
	for k := range out {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := map[string]any{}
	for _, k := range keys {
		ordered[k] = out[k]
	}
	return mustMarshalNode(ordered)
}

// listenerName 把节点名转成合法的 listener 名。
//
// 有两点必须同时满足：
//  1. 只含 mihomo 接受的字符（字母数字 - _）
//  2. 全局唯一 —— listener 名字重复会让内核直接 fatal 退出
//
// 只做字符替换是不够的：真实机场的节点名几乎全是中文/emoji，
// 替换后会退化成一堆下划线，例如
//
//	"🇺🇸美国高速01|CTCU|0.1x"  →  out-______01_CTCU_0_1x
//
// 多个国家的中文名替换后完全相同，内核报
// "listener ... is the duplicate name" 后直接退出。
// 因此这里追加节点名的哈希后缀，保证唯一且跨进程稳定。
func listenerName(node string) string {
	var sb strings.Builder
	sb.WriteString("out-")
	ascii := 0
	for _, r := range node {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
			ascii++
		default:
			sb.WriteByte('_')
		}
	}
	// 可读部分过长时截断，避免超长名字
	name := sb.String()
	if len(name) > 48 {
		name = name[:48]
	}
	// 哈希后缀保证唯一；截断后也必须带，否则仍可能撞名
	return fmt.Sprintf("%s-%08x", name, fnvHash(node))
}

// fnvHash 返回节点名的稳定哈希（FNV-1a 32 位）。
func fnvHash(s string) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
}

// listenAddr 决定端口绑定地址。
func listenAddr(allowLAN bool) string {
	if allowLAN {
		return "0.0.0.0"
	}
	return "127.0.0.1"
}

// localProxyURL 给本地端口拼出可用的代理 URL。
func (o *Options) localProxyURL(port int) string {
	host := "127.0.0.1"
	switch o.Mode {
	case ModeSocks:
		return "socks5://" + host + ":" + strconv.Itoa(port)
	case ModeHTTP:
		return "http://" + host + ":" + strconv.Itoa(port)
	default:
		// mixed 两种协议都收，socks5 对 Go 的 http.Transport 支持最稳
		return "socks5://" + host + ":" + strconv.Itoa(port)
	}
}

// writeConfig 把生成的配置原子落到 WorkDir。
func (o *Options) writeConfig(text string) (string, error) {
	if err := os.MkdirAll(o.WorkDir, 0o755); err != nil {
		return "", fmt.Errorf("mihomo-pool: 创建 WorkDir 失败: %w", err)
	}
	final := filepath.Join(o.WorkDir, o.ConfigFile)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, []byte(text), 0o600); err != nil {
		return "", fmt.Errorf("mihomo-pool: 写配置失败: %w", err)
	}
	// Windows 下目标文件可能被占用，先删再改名
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(final)
		if err2 := os.Rename(tmp, final); err2 != nil {
			_ = os.Remove(tmp)
			return "", fmt.Errorf("mihomo-pool: 落配置失败: %w", err)
		}
	}
	return final, nil
}

func sanitizeFileName(s string) string {
	var sb strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteByte('_')
		}
	}
	if sb.Len() == 0 {
		return "provider"
	}
	return sb.String()
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

func mustScalar(v any) *yaml.Node {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		panic("mihomo-pool: 编码标量失败: " + err.Error())
	}
	return &n
}

func mustMarshalNode(v any) *yaml.Node {
	var n yaml.Node
	if err := n.Encode(v); err != nil {
		panic("mihomo-pool: 编码配置段失败: " + err.Error())
	}
	return &n
}
