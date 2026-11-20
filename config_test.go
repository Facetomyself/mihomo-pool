package mihomopool

import (
	"strings"
	"testing"
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
