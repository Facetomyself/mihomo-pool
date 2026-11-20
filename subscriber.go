package mihomopool

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// 订阅抓取与节点解析。
//
// 两条路线：
//   1) 交内核自拉（ProviderSpec）：推荐。订阅 URL 只出现在内核配置里，
//      由 mihomo 自己定时更新，模块不参与内容解析。
//   2) 模块自己拉（FetchSubscription）：需要在起内核之前就知道节点名，
//      才能给每个节点预分配独立端口。此时解析成 NodeSpec 写进配置。
//
// 支持格式：Clash/mihomo YAML（proxies / proxy-providers）、
// 整包 base64 包裹的 YAML、以及 v2ray 分享链接（ss/vmess/trojan/hysteria2/tuic/...）。
// 分享链接能转成 ConfigItem 的按类型落地，转不了的按协议计数跳过。

// FetchOptions 控制订阅抓取。
type FetchOptions struct {
	// UserAgent 默认 clash-verge/2.0（多数机场按 UA 决定返回格式）。
	UserAgent string
	// Timeout 默认 25s。
	Timeout time.Duration
	// MaxBytes 默认 4 MiB。
	MaxBytes int64
	// Header 是额外请求头，可带机场的鉴权 token。
	Header map[string]string
	// Parse 是解析清洗选项。零值 = 丢弃信息节点 + 去重（推荐）。
	Parse ParseOptions
}

// FetchSubscription 拉取订阅正文并按格式解析成节点列表。
func FetchSubscription(ctx context.Context, rawURL string, fo FetchOptions) ([]NodeSpec, error) {
	body, err := fetchSubBody(ctx, rawURL, fo)
	if err != nil {
		return nil, err
	}
	// 默认走清洗：真实机场普遍会塞信息节点和重复条目
	return ParseSubscriptionWithOptions(body, fo.Parse)
}

// fetchSubBody 拉订阅原文。
func fetchSubBody(ctx context.Context, rawURL string, fo FetchOptions) ([]byte, error) {
	if strings.TrimSpace(rawURL) == "" {
		return nil, errors.New("mihomo-pool: 订阅地址为空")
	}
	ua := fo.UserAgent
	if ua == "" {
		ua = "clash-verge/2.0"
	}
	timeout := fo.Timeout
	if timeout <= 0 {
		timeout = 25 * time.Second
	}
	maxBytes := fo.MaxBytes
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, fmt.Errorf("mihomo-pool: 构造订阅请求失败: %w", err)
	}
	req.Header.Set("User-Agent", ua)
	for k, v := range fo.Header {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("mihomo-pool: 拉取订阅失败: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("mihomo-pool: 订阅返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return nil, fmt.Errorf("mihomo-pool: 读订阅正文失败: %w", err)
	}
	if len(body) == 0 {
		return nil, errors.New("mihomo-pool: 订阅正文为空")
	}
	return body, nil
}

// ParseSubscription 把订阅正文解析成节点列表。
func ParseSubscription(body []byte) ([]NodeSpec, error) {
	text := strings.TrimSpace(string(body))

	// 1) 整包 base64 包裹的 YAML：机场常见
	if decoded, ok := tryBase64Package(text); ok {
		if nodes, err := parseClashYAML(decoded); err == nil && len(nodes) > 0 {
			return nodes, nil
		}
		text = decoded
	}

	// 2) 直接是 YAML
	if strings.Contains(text, "proxies:") || strings.Contains(text, "proxy-providers:") {
		nodes, err := parseClashYAML(text)
		if err != nil {
			return nil, err
		}
		if len(nodes) > 0 {
			return nodes, nil
		}
	}

	// 3) 分享链接（一行一个）
	if nodes := parseShareLinks(text); len(nodes) > 0 {
		return nodes, nil
	}

	return nil, errors.New("mihomo-pool: 订阅既不包含 proxies/proxy-providers，也没有可解析的分享链接")
}

// ParseOptions 控制订阅解析的清洗行为。
type ParseOptions struct {
	// KeepInfoNodes 为 true 时保留机场投放的信息节点（剩余流量/套餐到期等）。
	// 默认丢弃：它们和真节点连接参数完全相同，留着只会白占端口、抢流量。
	KeepInfoNodes bool
	// KeepDuplicates 为 true 时保留连接参数完全相同的多条节点。
	// 默认丢弃后出现的重复项（保留第一条）。
	KeepDuplicates bool
}

// ParseSubscriptionWithOptions 是 ParseSubscription 的可配置版本。
func ParseSubscriptionWithOptions(body []byte, po ParseOptions) ([]NodeSpec, error) {
	nodes, err := ParseSubscription(body)
	if err != nil {
		return nil, err
	}
	return CleanNodes(nodes, po), nil
}

// CleanNodes 按选项清洗节点列表。
//
// 返回的节点顺序稳定（保留首次出现的位置），并保证名字唯一。
func CleanNodes(nodes []NodeSpec, po ParseOptions) []NodeSpec {
	if len(nodes) == 0 {
		return nodes
	}
	seenName := make(map[string]int, len(nodes))
	seenFP := make(map[string]string, len(nodes))
	out := make([]NodeSpec, 0, len(nodes))

	for _, n := range nodes {
		if !po.KeepInfoNodes && InfoNodeName(n.Name) {
			continue
		}
		fp := connFingerprint(n.Raw)
		if fp != "" && !po.KeepDuplicates {
			if prev, dup := seenFP[fp]; dup {
				_ = prev // 保留先出现的那个
				continue
			}
			seenFP[fp] = n.Name
		}
		if cnt, dup := seenName[n.Name]; dup {
			seenName[n.Name] = cnt + 1
			n.Name = fmt.Sprintf("%s-%d", n.Name, cnt+1)
			// 改名后要同步进 Raw，否则配置里仍是旧名
			if n.Raw != nil {
				n.Raw = cloneRaw(n.Raw)
				n.Raw["name"] = n.Name
			}
		} else {
			seenName[n.Name] = 1
		}
		out = append(out, n)
	}
	return out
}

// cloneRaw 浅拷贝地图，避免改到调用方的数据。
func cloneRaw(in map[string]any) map[string]any {
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// tryBase64Package 尝试把整包解成文本。
func tryBase64Package(text string) (string, bool) {
	if strings.Contains(text, "proxies:") || strings.Contains(text, "://") {
		return "", false
	}
	clean := strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', ' ', '\t':
			return -1
		}
		return r
	}, text)
	if len(clean) < 32 {
		return "", false
	}
	dec, err := decodeBase64Flexible(clean)
	if err != nil {
		return "", false
	}
	s := string(dec)
	if strings.Contains(s, "proxies:") || strings.Contains(s, "://") {
		return s, true
	}
	return "", false
}

// decodeBase64Flexible 兼容标准/URL 安全、带/不带 padding。
func decodeBase64Flexible(s string) ([]byte, error) {
	encs := []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	}
	var lastErr error
	for _, e := range encs {
		if b, err := e.DecodeString(s); err == nil {
			return b, nil
		} else {
			lastErr = err
		}
	}
	return nil, lastErr
}

// clashDoc 是订阅 YAML 的顶层结构（只取需要的字段）。
type clashDoc struct {
	Proxies []map[string]any `yaml:"proxies"`
}

// parseClashYAML 解析 Clash/mihomo YAML 的 proxies 段。
func parseClashYAML(text string) ([]NodeSpec, error) {
	var doc clashDoc
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil {
		return nil, fmt.Errorf("mihomo-pool: 解析订阅 YAML 失败: %w", err)
	}
	out := make([]NodeSpec, 0, len(doc.Proxies))
	seen := map[string]int{}
	for _, p := range doc.Proxies {
		name, _ := p["name"].(string)
		typ, _ := p["type"].(string)
		if strings.TrimSpace(name) == "" || strings.TrimSpace(typ) == "" {
			continue
		}
		// 节点名去重：重名会覆盖 listener 端口映射
		if n, dup := seen[name]; dup {
			seen[name] = n + 1
			name = name + "-" + strconv.Itoa(n+1)
		} else {
			seen[name] = 1
		}
		ns := NodeSpec{Name: name, Type: typ, Raw: p}
		if s, ok := p["server"].(string); ok {
			ns.Server = s
		}
		switch v := p["port"].(type) {
		case int:
			ns.Port = v
		case int64:
			ns.Port = int(v)
		case float64:
			ns.Port = int(v)
		case string:
			if iv, err := strconv.Atoi(v); err == nil {
				ns.Port = iv
			}
		}
		if g, ok := p["group"].(string); ok {
			ns.Group = g
		}
		if len(ns.Raw) == 0 {
			ns.Raw = map[string]any{"name": name, "type": typ}
		}
		out = append(out, ns)
	}
	return out, nil
}

// InfoNodeName 判断节点名是否是机场投放的信息节点。
//
// 真实机场常把"剩余流量/套餐到期/到期时间"这类公告塞进 proxies，
// 它们和某个真节点使用完全相同的连接参数，只是换了名字。
// 这类条目当成节点用会有三个后果：
//   - 白占一个独立端口
//   - 测速后可能排到前面，抢走真节点的流量（它们指向同一台机器）
//   - 让"节点数"失真，用户以为有 46 个出口其实只有 43 个
func InfoNodeName(name string) bool {
	n := strings.TrimSpace(name)
	if n == "" {
		return false
	}
	// 中文冒号或英文冒号后跟数字量，基本只出现在公告里
	for _, kw := range []string{
		"剩余流量", "剩余：", "套餐到期", "到期时间", "下次重置",
		"重置剩余", "距离下次", "过期时间", "流量重置", "官网", "订阅",
	} {
		if strings.Contains(n, kw) {
			return true
		}
	}
	return false
}

// connFingerprint 生成节点的连接指纹，用于识别同一出口的重复条目。
//
// 指纹只取决定"连到哪台机器、用什么身份"的字段，不含 name。
// 两个节点指纹相同 = 出口完全一样，保留一个即可。
func connFingerprint(p map[string]any) string {
	var sb strings.Builder
	for _, k := range []string{
		"type", "server", "port", "uuid", "password",
		"cipher", "network", "servername", "sni", "flow",
	} {
		v, ok := p[k]
		if !ok {
			continue
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(strings.ToLower(strings.TrimSpace(fmt.Sprint(v))))
		sb.WriteByte('&')
	}
	return sb.String()
}

// parseShareLinks 解析一行一个的分享链接。
func parseShareLinks(text string) []NodeSpec {
	var out []NodeSpec
	seen := map[string]int{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, "://") {
			continue
		}
		spec, ok := parseShareLink(line)
		if !ok {
			continue
		}
		if n, dup := seen[spec.Name]; dup {
			seen[spec.Name] = n + 1
			spec.Name = spec.Name + "-" + strconv.Itoa(n+1)
		} else {
			seen[spec.Name] = 1
		}
		out = append(out, spec)
	}
	return out
}

// parseShareLink 把一条分享链接转成 NodeSpec。
func parseShareLink(link string) (NodeSpec, bool) {
	switch {
	case strings.HasPrefix(link, "ss://"):
		return parseSS(link)
	case strings.HasPrefix(link, "vmess://"):
		return parseVMess(link)
	case strings.HasPrefix(link, "trojan://"):
		return parseTrojan(link)
	case strings.HasPrefix(link, "hysteria2://"), strings.HasPrefix(link, "hy2://"):
		return parseHysteria2(link)
	case strings.HasPrefix(link, "tuic://"):
		return parseTUIC(link)
	case strings.HasPrefix(link, "socks5://"), strings.HasPrefix(link, "http://"), strings.HasPrefix(link, "https://"):
		return parsePlainProxy(link)
	}
	return NodeSpec{}, false
}

// parseSS 解析 ss:// 链接，同时支持 SIP002 与旧式 base64(method:pass@host:port)。
func parseSS(link string) (NodeSpec, bool) {
	rest := strings.TrimPrefix(link, "ss://")
	// 去掉片段（#名称）
	name := ""
	if i := strings.LastIndex(rest, "#"); i >= 0 {
		name, _ = url.PathUnescape(rest[i+1:])
		rest = rest[:i]
	}
	// SIP002: base64(method:pass)@host:port?plugin=...
	if at := strings.LastIndex(rest, "@"); at >= 0 && strings.Contains(rest[at+1:], ":") {
		userinfo := rest[:at]
		hostport := rest[at+1:]
		hostport = strings.SplitN(hostport, "/", 2)[0]
		query := ""
		if q := strings.Index(hostport, "?"); q >= 0 {
			query = hostport[q+1:]
			hostport = hostport[:q]
		}
		cred, err := decodeBase64Flexible(userinfo)
		if err != nil {
			return NodeSpec{}, false
		}
		parts := strings.SplitN(string(cred), ":", 2)
		if len(parts) != 2 {
			return NodeSpec{}, false
		}
		host, port, ok := splitHostPort(hostport)
		if !ok {
			return NodeSpec{}, false
		}
		raw := map[string]any{
			"name": nameOf(name, "ss", host, port),
			"type": "ss", "server": host, "port": port,
			"cipher": parts[0], "password": parts[1],
		}
		if query != "" {
			if v, err := url.ParseQuery(query); err == nil {
				if plug := v.Get("plugin"); plug != "" {
					raw["plugin"] = plug
					if opts := v.Get("plugin-opts"); opts != "" {
						raw["plugin-opts"] = opts
					}
				}
			}
		}
		return NodeSpec{Name: raw["name"].(string), Type: "ss", Server: host, Port: port, Raw: raw}, true
	}
	// 旧式：整段 base64
	dec, err := decodeBase64Flexible(rest)
	if err != nil {
		return NodeSpec{}, false
	}
	s := string(dec)
	at := strings.LastIndex(s, "@")
	if at < 0 {
		return NodeSpec{}, false
	}
	cred := strings.SplitN(s[:at], ":", 2)
	if len(cred) != 2 {
		return NodeSpec{}, false
	}
	host, port, ok := splitHostPort(s[at+1:])
	if !ok {
		return NodeSpec{}, false
	}
	raw := map[string]any{
		"name": nameOf(name, "ss", host, port), "type": "ss",
		"server": host, "port": port, "cipher": cred[0], "password": cred[1],
	}
	return NodeSpec{Name: raw["name"].(string), Type: "ss", Server: host, Port: port, Raw: raw}, true
}

// parseVMess 解析 vmess://（标准 base64 JSON）。
func parseVMess(link string) (NodeSpec, bool) {
	payload := strings.TrimPrefix(link, "vmess://")
	if i := strings.LastIndex(payload, "#"); i >= 0 {
		payload = payload[:i]
	}
	dec, err := decodeBase64Flexible(payload)
	if err != nil {
		return NodeSpec{}, false
	}
	var v struct {
		V    string `json:"v"`
		PS   string `json:"ps"`
		Add  string `json:"add"`
		Port any    `json:"port"`
		ID   string `json:"id"`
		Aid  any    `json:"aid"`
		Net  string `json:"net"`
		Type string `json:"type"`
		Host string `json:"host"`
		Path string `json:"path"`
		TLS  string `json:"tls"`
		SNI  string `json:"sni"`
		Alpn string `json:"alpn"`
	}
	if err := json.Unmarshal(dec, &v); err != nil {
		return NodeSpec{}, false
	}
	port := toInt(v.Port)
	if v.Add == "" || port == 0 || v.ID == "" {
		return NodeSpec{}, false
	}
	name := nameOf(v.PS, "vmess", v.Add, port)
	raw := map[string]any{
		"name": name, "type": "vmess", "server": v.Add, "port": port,
		"uuid": v.ID, "alterId": toInt(v.Aid), "cipher": "auto",
	}
	if v.Net != "" {
		raw["network"] = v.Net
	}
	if v.TLS == "tls" {
		raw["tls"] = true
		if v.SNI != "" {
			raw["servername"] = v.SNI
		}
		if v.Alpn != "" {
			raw["alpn"] = splitCSV(v.Alpn)
		}
	}
	if v.Net == "ws" {
		ws := map[string]any{}
		if v.Path != "" {
			ws["path"] = v.Path
		}
		if v.Host != "" {
			ws["headers"] = map[string]any{"Host": v.Host}
		}
		if len(ws) > 0 {
			raw["ws-opts"] = ws
		}
	}
	if v.Net == "grpc" && v.Path != "" {
		raw["grpc-opts"] = map[string]any{"grpc-service-name": v.Path}
	}
	return NodeSpec{Name: name, Type: "vmess", Server: v.Add, Port: port, Raw: raw}, true
}

// parseTrojan 解析 trojan://password@host:port?params#name。
func parseTrojan(link string) (NodeSpec, bool) {
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return NodeSpec{}, false
	}
	pass := u.User.Username()
	if pass == "" {
		return NodeSpec{}, false
	}
	host, port, ok := splitHostPort(u.Host)
	if !ok {
		return NodeSpec{}, false
	}
	name := nameOf(u.Fragment, "trojan", host, port)
	raw := map[string]any{
		"name": name, "type": "trojan", "server": host, "port": port, "password": pass,
	}
	q := u.Query()
	if q.Get("sni") != "" {
		raw["sni"] = q.Get("sni")
	} else if q.Get("peer") != "" {
		raw["sni"] = q.Get("peer")
	}
	if q.Get("allowInsecure") == "1" || q.Get("allowInsecure") == "true" {
		raw["skip-cert-verify"] = true
	}
	if q.Get("type") == "ws" {
		ws := map[string]any{}
		if p := q.Get("path"); p != "" {
			ws["path"] = p
		}
		if h := firstNonEmpty(q.Get("host"), q.Get("sni")); h != "" {
			ws["headers"] = map[string]any{"Host": h}
		}
		if len(ws) > 0 {
			raw["network"] = "ws"
			raw["ws-opts"] = ws
		}
	}
	if q.Get("type") == "grpc" {
		raw["network"] = "grpc"
		if sn := q.Get("serviceName"); sn != "" {
			raw["grpc-opts"] = map[string]any{"grpc-service-name": sn}
		}
	}
	return NodeSpec{Name: name, Type: "trojan", Server: host, Port: port, Raw: raw}, true
}

// parseHysteria2 解析 hysteria2:// / hy2://。
func parseHysteria2(link string) (NodeSpec, bool) {
	link = strings.Replace(link, "hy2://", "hysteria2://", 1)
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return NodeSpec{}, false
	}
	host, port, ok := splitHostPort(u.Host)
	if !ok {
		return NodeSpec{}, false
	}
	name := nameOf(u.Fragment, "hy2", host, port)
	raw := map[string]any{
		"name": name, "type": "hysteria2", "server": host, "port": port,
	}
	if pass := u.User.Username(); pass != "" {
		raw["password"] = pass
	}
	q := u.Query()
	if q.Get("sni") != "" {
		raw["sni"] = q.Get("sni")
	}
	if q.Get("insecure") == "1" || q.Get("insecure") == "true" {
		raw["skip-cert-verify"] = true
	}
	if q.Get("obfs") != "" {
		raw["obfs"] = q.Get("obfs")
		if q.Get("obfs-password") != "" {
			raw["obfs-password"] = q.Get("obfs-password")
		}
	}
	return NodeSpec{Name: name, Type: "hysteria2", Server: host, Port: port, Raw: raw}, true
}

// parseTUIC 解析 tuic://uuid:password@host:port。
func parseTUIC(link string) (NodeSpec, bool) {
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return NodeSpec{}, false
	}
	host, port, ok := splitHostPort(u.Host)
	if !ok {
		return NodeSpec{}, false
	}
	name := nameOf(u.Fragment, "tuic", host, port)
	raw := map[string]any{
		"name": name, "type": "tuic", "server": host, "port": port,
	}
	if uuid := u.User.Username(); uuid != "" {
		raw["uuid"] = uuid
	}
	if pass, ok := u.User.Password(); ok {
		raw["password"] = pass
	}
	q := u.Query()
	if q.Get("sni") != "" {
		raw["sni"] = q.Get("sni")
	}
	if q.Get("congestion_control") != "" {
		raw["congestion-controller"] = q.Get("congestion_control")
	}
	return NodeSpec{Name: name, Type: "tuic", Server: host, Port: port, Raw: raw}, true
}

// parsePlainProxy 解析裸 http/socks5 代理链接。
func parsePlainProxy(link string) (NodeSpec, bool) {
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return NodeSpec{}, false
	}
	host, port, ok := splitHostPort(u.Host)
	if !ok {
		return NodeSpec{}, false
	}
	typ := "http"
	if strings.HasPrefix(link, "socks5://") {
		typ = "socks5"
	}
	name := nameOf(u.Fragment, typ, host, port)
	raw := map[string]any{
		"name": name, "type": typ, "server": host, "port": port,
	}
	if u.User != nil {
		if user := u.User.Username(); user != "" {
			raw["username"] = user
		}
		if pass, ok := u.User.Password(); ok {
			raw["password"] = pass
		}
		if typ == "http" || typ == "socks5" {
			raw["tls"] = false
		}
	}
	return NodeSpec{Name: name, Type: typ, Server: host, Port: port, Raw: raw}, true
}

func nameOf(fragment, typ, host string, port int) string {
	f := strings.TrimSpace(fragment)
	if f != "" {
		return f
	}
	return typ + "-" + host + "-" + strconv.Itoa(port)
}

func splitHostPort(hostport string) (string, int, bool) {
	hostport = strings.TrimSpace(strings.TrimSuffix(hostport, "/"))
	if hostport == "" {
		return "", 0, false
	}
	host, portStr, err := splitHostPortStrict(hostport)
	if err != nil {
		return "", 0, false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, false
	}
	return host, port, true
}

// splitHostPortStrict 兼容 IPv6 字面量与普通 host:port。
func splitHostPortStrict(hostport string) (string, string, error) {
	if strings.HasPrefix(hostport, "[") {
		i := strings.LastIndex(hostport, "]:")
		if i < 0 {
			return "", "", errors.New("缺少端口")
		}
		return hostport[1:i], hostport[i+2:], nil
	}
	i := strings.LastIndex(hostport, ":")
	if i < 0 {
		return "", "", errors.New("缺少端口")
	}
	return hostport[:i], hostport[i+1:], nil
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int64:
		return int(x)
	case float64:
		return int(x)
	case string:
		if n, err := strconv.Atoi(strings.TrimSpace(x)); err == nil {
			return n
		}
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return int(n)
		}
	}
	return 0
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
