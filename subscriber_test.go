package mihomopool

import (
	"encoding/base64"
	"strings"
	"testing"
)

const clashYAML = `
port: 7890
socks-port: 7891
proxies:
  - name: 香港 01
    type: ss
    server: hk1.example.com
    port: 8388
    cipher: aes-256-gcm
    password: pass1
  - name: 日本 02
    type: trojan
    server: jp1.example.com
    port: 443
    password: pass2
    sni: jp1.example.com
`

func TestParseClashYAML(t *testing.T) {
	nodes, err := ParseSubscription([]byte(clashYAML))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
	if nodes[0].Name != "香港 01" || nodes[0].Type != "ss" {
		t.Fatalf("第一个节点不对: %+v", nodes[0])
	}
	if nodes[0].Port != 8388 || nodes[0].Server != "hk1.example.com" {
		t.Fatalf("第一个节点地址不对: %+v", nodes[0])
	}
	if nodes[1].Type != "trojan" {
		t.Fatalf("第二个节点类型不对: %+v", nodes[1])
	}
}

func TestParseBase64WrappedYAML(t *testing.T) {
	packed := base64.StdEncoding.EncodeToString([]byte(clashYAML))
	nodes, err := ParseSubscription([]byte(packed))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
}

func TestParseBase64URLWrappedNoPadding(t *testing.T) {
	packed := base64.RawURLEncoding.EncodeToString([]byte(clashYAML))
	nodes, err := ParseSubscription([]byte(packed))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
}

func TestParseSSShareLink(t *testing.T) {
	// SIP002: ss://base64(method:pass)@host:port#name
	cred := base64.StdEncoding.EncodeToString([]byte("aes-256-gcm:secret"))
	link := "ss://" + cred + "@ss.example.com:8443#ss-node"
	nodes, err := ParseSubscription([]byte(link))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("期望 1 个节点，得到 %d", len(nodes))
	}
	n := nodes[0]
	if n.Type != "ss" || n.Server != "ss.example.com" || n.Port != 8443 {
		t.Fatalf("节点不对: %+v", n)
	}
	if n.Raw["cipher"] != "aes-256-gcm" || n.Raw["password"] != "secret" {
		t.Fatalf("凭据解析不对: %+v", n.Raw)
	}
	if n.Name != "ss-node" {
		t.Fatalf("名称解析不对: %q", n.Name)
	}
}

func TestParseSSLegacyBase64(t *testing.T) {
	inner := base64.StdEncoding.EncodeToString([]byte("chacha20-ietf-poly1305:pw@old.example.com:9999"))
	nodes, err := ParseSubscription([]byte("ss://" + inner))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 1 || nodes[0].Port != 9999 {
		t.Fatalf("旧式 ss 解析失败: %+v", nodes)
	}
}

func TestParseVMessShareLink(t *testing.T) {
	payload := `{"v":"2","ps":"vm节点","add":"vm.example.com","port":"443","id":"11111111-2222-3333-4444-555555555555","aid":"0","net":"ws","type":"none","host":"cdn.example.com","path":"/ray","tls":"tls"}`
	link := "vmess://" + base64.StdEncoding.EncodeToString([]byte(payload))
	nodes, err := ParseSubscription([]byte(link))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("期望 1 个节点，得到 %d", len(nodes))
	}
	n := nodes[0]
	if n.Type != "vmess" || n.Port != 443 || n.Server != "vm.example.com" {
		t.Fatalf("vmess 解析不对: %+v", n)
	}
	if n.Raw["uuid"] != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("uuid 解析不对: %+v", n.Raw)
	}
	if n.Raw["tls"] != true {
		t.Fatalf("tls 解析不对: %+v", n.Raw)
	}
	ws, ok := n.Raw["ws-opts"].(map[string]any)
	if !ok || ws["path"] != "/ray" {
		t.Fatalf("ws-opts 解析不对: %+v", n.Raw)
	}
}

func TestParseTrojanAndHysteria2(t *testing.T) {
	links := strings.Join([]string{
		"trojan://pass@trojan.example.com:443?sni=t.example.com&allowInsecure=1#tj",
		"hysteria2://pw@hy.example.com:8443?sni=hy.example.com#hy",
		"hy2://pw2@hy2.example.com:9443#hy2",
		"tuic://uuid-x:pw3@tuic.example.com:443?sni=tu.example.com#tu",
	}, "\n")
	nodes, err := ParseSubscription([]byte(links))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 4 {
		t.Fatalf("期望 4 个节点，得到 %d", len(nodes))
	}
	types := map[string]bool{}
	for _, n := range nodes {
		types[n.Type] = true
	}
	for _, want := range []string{"trojan", "hysteria2", "tuic"} {
		if !types[want] {
			t.Fatalf("缺少类型 %s，实际 %v", want, types)
		}
	}
	if nodes[0].Raw["skip-cert-verify"] != true {
		t.Fatalf("allowInsecure 未转换: %+v", nodes[0].Raw)
	}
}

func TestParseDuplicateNamesDedup(t *testing.T) {
	y := `
proxies:
  - {name: dup, type: ss, server: a.com, port: 1, cipher: aes-128-gcm, password: x}
  - {name: dup, type: ss, server: b.com, port: 2, cipher: aes-128-gcm, password: y}
`
	nodes, err := ParseSubscription([]byte(y))
	if err != nil {
		t.Fatalf("ParseSubscription: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
	if nodes[0].Name == nodes[1].Name {
		t.Fatalf("重名未去重: %q / %q", nodes[0].Name, nodes[1].Name)
	}
}

func TestParseInvalidSubscription(t *testing.T) {
	if _, err := ParseSubscription([]byte("这不是订阅")); err == nil {
		t.Fatal("期望解析失败")
	}
}

func TestSplitHostPortIPv6(t *testing.T) {
	host, port, ok := splitHostPort("[2001:db8::1]:8080")
	if !ok || host != "2001:db8::1" || port != 8080 {
		t.Fatalf("IPv6 解析不对: %q %d %v", host, port, ok)
	}
	if _, _, ok := splitHostPort("no-port"); ok {
		t.Fatal("无端口应失败")
	}
}

func TestInfoNodeNameDetection(t *testing.T) {
	info := []string{
		"剩余流量：878.1 GB",
		"距离下次重置剩余：22 天",
		"套餐到期：2026-12-12",
		"到期时间: 2026-01-01",
		"官网 https://example.com",
	}
	for _, n := range info {
		if !InfoNodeName(n) {
			t.Fatalf("%q 应识别为信息节点", n)
		}
	}
	real := []string{
		"🇭🇰香港高速01|BGP|CMCU",
		"🇯🇵日本专线03|BGP|流媒体",
		"us-node-1",
		"",
	}
	for _, n := range real {
		if InfoNodeName(n) {
			t.Fatalf("%q 被误判为信息节点", n)
		}
	}
}

func TestCleanNodesDropsInfoNodes(t *testing.T) {
	// 真实机场形态：3 条信息节点 + 1 个真节点，四者连接参数完全相同
	base := map[string]any{
		"type": "vless", "server": "example-airport.test", "port": 443,
		"uuid": "65165087-6d7b-4b57-9b6b-000000000000", "flow": "xtls-rprx-vision",
	}
	mk := func(name string) NodeSpec {
		raw := cloneRaw(base)
		raw["name"] = name
		return NodeSpec{Name: name, Type: "vless", Server: "example-airport.test", Port: 443, Raw: raw}
	}
	nodes := []NodeSpec{
		mk("剩余流量：878.1 GB"),
		mk("🚀 距离下次重置剩余：22 天"),
		mk("📅 套餐到期：2026-12-12"),
		mk("🇭🇰香港高速01|BGP|CMCU"),
	}
	got := CleanNodes(nodes, ParseOptions{})
	if len(got) != 1 {
		t.Fatalf("期望只剩 1 个节点，得到 %d: %v", len(got), nodeNames(got))
	}
	if got[0].Name != "🇭🇰香港高速01|BGP|CMCU" {
		t.Fatalf("留下的不是真节点: %q", got[0].Name)
	}
}

func TestCleanNodesDedupKeepsFirst(t *testing.T) {
	mk := func(name, server, uuid string) NodeSpec {
		raw := map[string]any{
			"name": name, "type": "vless", "server": server, "port": 443, "uuid": uuid,
		}
		return NodeSpec{Name: name, Type: "vless", Server: server, Port: 443, Raw: raw}
	}
	nodes := []NodeSpec{
		mk("hk-1", "a.example.com", "uuid-1"),
		mk("hk-1-duplicate", "a.example.com", "uuid-1"), // 同参数，不同名
		mk("jp-1", "b.example.com", "uuid-2"),           // 不同参数
	}
	got := CleanNodes(nodes, ParseOptions{})
	if len(got) != 2 {
		t.Fatalf("期望去重后 2 个，得到 %d: %v", len(got), nodeNames(got))
	}
	if got[0].Name != "hk-1" {
		t.Fatalf("应保留先出现的: %q", got[0].Name)
	}
}

func TestCleanNodesKeepOptions(t *testing.T) {
	mk := func(name string) NodeSpec {
		raw := map[string]any{"name": name, "type": "ss", "server": "x", "port": 1}
		return NodeSpec{Name: name, Type: "ss", Server: "x", Port: 1, Raw: raw}
	}
	nodes := []NodeSpec{mk("剩余流量：878 GB"), mk("hk-1"), mk("hk-1")}

	// 保留信息节点 + 保留重复
	got := CleanNodes(nodes, ParseOptions{KeepInfoNodes: true, KeepDuplicates: true})
	if len(got) != 3 {
		t.Fatalf("全保留时应 3 个，得到 %d", len(got))
	}
	// 即使保留重复，名字也要唯一
	seen := map[string]bool{}
	for _, n := range got {
		if seen[n.Name] {
			t.Fatalf("名字未唯一化: %q", n.Name)
		}
		seen[n.Name] = true
	}
}

func TestCleanNodesRenamesIntoRaw(t *testing.T) {
	// 重名节点被改名后，Raw 里的 name 必须同步，否则写进配置的还是旧名
	raw1 := map[string]any{"name": "dup", "type": "ss", "server": "a", "port": 1}
	raw2 := map[string]any{"name": "dup", "type": "ss", "server": "b", "port": 2}
	nodes := []NodeSpec{
		{Name: "dup", Type: "ss", Server: "a", Port: 1, Raw: raw1},
		{Name: "dup", Type: "ss", Server: "b", Port: 2, Raw: raw2},
	}
	got := CleanNodes(nodes, ParseOptions{})
	if len(got) != 2 {
		t.Fatalf("期望 2 个，得到 %d", len(got))
	}
	if got[1].Raw["name"] != got[1].Name {
		t.Fatalf("Raw name 未同步: Raw=%v Name=%q", got[1].Raw["name"], got[1].Name)
	}
	// 不应改动调用方传进来的原 map
	if raw2["name"] != "dup" {
		t.Fatal("污染了调用方的 Raw")
	}
}

func nodeNames(ns []NodeSpec) []string {
	out := make([]string, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.Name)
	}
	return out
}
