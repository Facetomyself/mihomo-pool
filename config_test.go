package mihomopool

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestOptionsNormalizeDefaults(t *testing.T) {
	o := Options{}
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if o.SocksPort != 17890 || o.APIPort != 17891 || o.BasePort != 17900 {
		t.Fatalf("默认端口不对: %+v", o)
	}
	if o.Mode != ModeMixed {
		t.Fatalf("默认 Mode 不对: %q", o.Mode)
	}
	if o.WorkDir == "" {
		t.Fatal("WorkDir 未落默认值")
	}
}

func TestOptionsRejectsBadMode(t *testing.T) {
	o := Options{Mode: "quic"}
	if err := o.normalize(); err == nil {
		t.Fatal("期望 Mode 校验失败")
	}
}

func TestOptionsRejectsSamePort(t *testing.T) {
	o := Options{SocksPort: 18000, APIPort: 18000}
	if err := o.normalize(); err == nil || !strings.Contains(err.Error(), "不能相同") {
		t.Fatalf("期望端口冲突报错，得到 %v", err)
	}
}

func TestOptionsRejectsPortOutOfRange(t *testing.T) {
	o := Options{BasePort: 70000}
	if err := o.normalize(); err == nil {
		t.Fatal("期望端口越界报错")
	}
}

func TestRenderConfigListenerPerNode(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.BasePort = 18000
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}

	nodes := []NodeSpec{
		{Name: "hk-1", Type: "ss", Server: "1.1.1.1", Port: 8388, Raw: map[string]any{"cipher": "aes-128-gcm", "password": "x"}},
		{Name: "us-1", Type: "trojan", Server: "2.2.2.2", Port: 443, Raw: map[string]any{"password": "y"}},
	}
	text, bindings, err := o.renderConfig(nodes, nil)
	if err != nil {
		t.Fatalf("renderConfig: %v", err)
	}

	if len(bindings) != 2 {
		t.Fatalf("期望 2 条绑定，得到 %d", len(bindings))
	}
	if bindings[0].Port != 18000 || bindings[1].Port != 18001 {
		t.Fatalf("端口分配不对: %+v", bindings)
	}
	if bindings[0].ProxyURL != "socks5://127.0.0.1:18000" {
		t.Fatalf("proxy url 不对: %s", bindings[0].ProxyURL)
	}

	for _, want := range []string{
		"mixed-port: 17890",
		"external-controller: 127.0.0.1:17891",
		"listeners:",
		"proxy: hk-1",
		"proxy: us-1",
		"port: 18000",
		"port: 18001",
		"listen: 127.0.0.1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("配置缺少 %q\n---\n%s", want, text)
		}
	}
	// 不允许出现空的 listeners 段或未绑定的端口
	if strings.Contains(text, "allow-lan: true") {
		t.Fatal("默认不应允许 LAN")
	}
}

func TestRenderConfigRejectsDuplicateNames(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	_ = o.normalize()
	_, _, err := o.renderConfig([]NodeSpec{
		{Name: "a", Type: "ss"},
		{Name: "a", Type: "ss"},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "重复") {
		t.Fatalf("期望重名报错，得到 %v", err)
	}
}

func TestRenderConfigProviders(t *testing.T) {
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	_ = o.normalize()
	text, bindings, err := o.renderConfig(nil, []ProviderSpec{
		{Name: "airport", URL: "https://example.com/sub", IntervalSec: 3600},
	})
	if err != nil {
		t.Fatalf("renderConfig: %v", err)
	}
	if len(bindings) != 0 {
		t.Fatal("provider 不应产生端口绑定")
	}
	for _, want := range []string{"proxy-providers:", "airport:", "https://example.com/sub", "interval: 3600"} {
		if !strings.Contains(text, want) {
			t.Fatalf("配置缺少 %q\n---\n%s", want, text)
		}
	}
	// 订阅原文不能出现在配置里，避免覆盖我们的端口设置
	if strings.Contains(text, "mixed-port: 7890") {
		t.Fatal("配置被订阅内容污染")
	}
}

func TestListenerNameSanitize(t *testing.T) {
	got := listenerName("香港 01/测试α")
	if strings.ContainsAny(got, " /α") {
		t.Fatalf("listener 名未净化: %q", got)
	}
	if !strings.HasPrefix(got, "out-") {
		t.Fatalf("listener 名缺前缀: %q", got)
	}
}

func TestLocalProxyURLByMode(t *testing.T) {
	cases := []struct {
		mode ProxyMode
		want string
	}{
		{ModeMixed, "socks5://127.0.0.1:19000"},
		{ModeSocks, "socks5://127.0.0.1:19000"},
		{ModeHTTP, "http://127.0.0.1:19000"},
	}
	for _, c := range cases {
		o := Options{Mode: c.mode}
		if got := o.localProxyURL(19000); got != c.want {
			t.Fatalf("%s → %s，期望 %s", c.mode, got, c.want)
		}
	}
}

func TestListenerNameUniqueForChineseNames(t *testing.T) {
	// 真实机场的节点名几乎全是中文/emoji。只做字符替换会让不同节点
	// 产生相同 listener 名，内核直接 fatal 退出。这里用真实数据回归。
	names := []string{
		"🇺🇸美国高速01|CTCU|0.1x",
		"🇬🇧英国伦敦01|CTCU|0.1x",
		"🇨🇳台湾高速01|BGP|流媒体",
		"🇺🇸美国洛杉矶01|流媒体|0.01x",
		"🇸🇬新加坡专线01|BGP|流媒体",
		"🇯🇵日本专线01|BGP|流媒体",
		"🇭🇰香港高速01|BGP|CMCU",
		"剩余流量：878.1 GB",
		"距离下次重置剩余：22 天",
		"套餐到期：2026-12-12",
	}
	seen := map[string]string{}
	for _, n := range names {
		l := listenerName(n)
		if prev, dup := seen[l]; dup {
			t.Fatalf("listener 名冲突: %q 与 %q 都映射到 %q", prev, n, l)
		}
		seen[l] = n
		// 必须是 mihomo 接受的字符集
		for _, r := range l {
			ok := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
				(r >= '0' && r <= '9') || r == '-' || r == '_'
			if !ok {
				t.Fatalf("%q 的 listener 名含非法字符 %q: %s", n, r, l)
			}
		}
		if len(l) > 64 {
			t.Fatalf("listener 名过长 (%d): %s", len(l), l)
		}
	}
}

func TestListenerNameStable(t *testing.T) {
	// 同一节点名必须每次得到同样的 listener 名，否则重载会改端口绑定
	a := listenerName("🇭🇰香港高速01|BGP|CMCU")
	b := listenerName("🇭🇰香港高速01|BGP|CMCU")
	if a != b {
		t.Fatalf("listener 名不稳定: %q vs %q", a, b)
	}
}

func TestRenderConfigRealisticNodes(t *testing.T) {
	// 用接近真实机场的一组节点跑一遍生成，确保无重复 listener
	o := DefaultOptions()
	o.WorkDir = t.TempDir()
	o.BasePort = 20000
	if err := o.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	var nodes []NodeSpec
	for i := 0; i < 46; i++ {
		nodes = append(nodes, NodeSpec{
			Name: "🇺🇸美国高速" + fmt.Sprintf("%02d", i) + "|CTCU|0.1x",
			Type: "vless", Server: "a.xyz", Port: 443,
			Raw: map[string]any{"uuid": "u"},
		})
	}
	text, bindings, err := o.renderConfig(nodes, nil)
	if err != nil {
		t.Fatalf("renderConfig: %v", err)
	}
	if len(bindings) != 46 {
		t.Fatalf("期望 46 条绑定，得到 %d", len(bindings))
	}
	// 解析回 YAML 检查 listeners 名字唯一
	var parsed struct {
		Listeners []listenerConfig `yaml:"listeners"`
	}
	if err := yaml.Unmarshal([]byte(text), &parsed); err != nil {
		t.Fatalf("解析生成配置失败: %v", err)
	}
	if len(parsed.Listeners) != 46 {
		t.Fatalf("listeners 数量不对: %d", len(parsed.Listeners))
	}
	seen := map[string]bool{}
	for _, l := range parsed.Listeners {
		if seen[l.Name] {
			t.Fatalf("listener 重名: %s", l.Name)
		}
		seen[l.Name] = true
	}
}
