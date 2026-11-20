package mihomopool

import (
	"testing"
	"time"
)

func TestIsGroupExcludesBuiltins(t *testing.T) {
	// 实测自 mihomo v1.19.34 的 /proxies 返回
	builtins := []ProxyItem{
		{Name: "COMPATIBLE", Type: "Compatible"},
		{Name: "DIRECT", Type: "Direct"},
		{Name: "GLOBAL", Type: "Selector", All: []string{"A", "B"}},
		{Name: "PASS", Type: "Pass"},
		{Name: "PASS-RULE", Type: "PassRule"},
		{Name: "REJECT", Type: "Reject"},
		{Name: "REJECT-DROP", Type: "RejectDrop"},
	}
	for _, p := range builtins {
		if !p.IsGroup() {
			t.Fatalf("%s(%s) 应被判定为非节点", p.Name, p.Type)
		}
	}

	real := []ProxyItem{
		{Name: "香港 01", Type: "Shadowsocks"},
		{Name: "us-1", Type: "Trojan"},
		{Name: "hy", Type: "Hysteria2"},
		{Name: "local", Type: "Socks5"},
		{Name: "http-node", Type: "Http"},
	}
	for _, p := range real {
		if p.IsGroup() {
			t.Fatalf("%s(%s) 应被判定为节点", p.Name, p.Type)
		}
	}
}

func TestIsGroupByAllField(t *testing.T) {
	// 任何带 all 的条目都是组
	p := ProxyItem{Name: "自定义组", Type: "SomethingNew", All: []string{"x"}}
	if !p.IsGroup() {
		t.Fatal("带 all 的条目应判定为组")
	}
}

func TestBuiltinNamesCaseInsensitive(t *testing.T) {
	for _, name := range []string{"direct", "Reject", "pass-rule", " Global "} {
		if !isBuiltinOutbound(name) {
			t.Fatalf("%q 应被识别为内置出站", name)
		}
	}
	if isBuiltinOutbound("香港01") {
		t.Fatal("真实节点名被误判")
	}
}

func TestProxyItemDelay(t *testing.T) {
	p := ProxyItem{}
	if p.Delay() != -1 {
		t.Fatal("无历史应返回 -1")
	}
	p.History = append(p.History, struct {
		Time  time.Time `json:"time"`
		Delay int64     `json:"delay"`
	}{Time: time.Now(), Delay: 88})
	if got := p.Delay(); got != 88 {
		t.Fatalf("应返回最近一次延迟 88，得到 %d", got)
	}
}
