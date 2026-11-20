package mihomopool

import (
	"strings"
	"testing"
)

func TestExtractTagsRealNodeNames(t *testing.T) {
	// 真实机场节点名（已脱敏域名），覆盖中英文、emoji、倍率、线路特性
	cases := []struct {
		name string
		typ  string
		want []string
		deny []string
	}{
		{
			name: "🇭🇰香港高速01|BGP|CMCU", typ: "vless",
			want: []string{"region:HK", "bgp", "speed", "proto:vless"},
		},
		{
			name: "🇯🇵日本专线03|BGP|流媒体", typ: "vless",
			want: []string{"region:JP", "dedicated", "stream", "bgp"},
		},
		{
			name: "🇺🇸美国洛杉矶01|流媒体|0.01x", typ: "vless",
			want: []string{"region:US", "rate:0.01", "stream"},
		},
		{
			name: "🇬🇧英国伦敦01|CTCU|0.1x", typ: "vless",
			want: []string{"region:GB", "rate:0.1"},
		},
		{
			name: "🇸🇬新加坡专线01|BGP|流媒体", typ: "hysteria2",
			want: []string{"region:SG", "dedicated", "stream", "proto:hysteria2"},
		},
		{
			name: "🇨🇳台湾高速01|BGP|流媒体", typ: "vless",
			want: []string{"region:TW", "speed", "stream"},
		},
		{
			name: "US-LosAngeles-Premium", typ: "ss",
			want: []string{"region:US"},
		},
		{
			name: "HK-01", typ: "ss",
			want: []string{"region:HK"},
		},
	}

	for _, c := range cases {
		got := ExtractTags(c.name, c.typ)
		for _, w := range c.want {
			if !HasTag(got, w) {
				t.Fatalf("%q 缺少标签 %s，实际 %v", c.name, w, got)
			}
		}
		for _, d := range c.deny {
			if HasTag(got, d) {
				t.Fatalf("%q 不应有标签 %s", c.name, d)
			}
		}
	}
}

// TestExtractTagsNoUnitFalsePositive 回归：GB 是吉字节，不是英国。
func TestExtractTagsNoUnitFalsePositive(t *testing.T) {
	units := []string{
		"剩余流量：878.1 GB",
		"剩余流量：512 MB",
		"总量 2 TB",
	}
	for _, n := range units {
		tags := ExtractTags(n, "vless")
		if HasTagPrefix(tags, "region:") {
			t.Fatalf("%q 不应识别出地区，实际 %v", n, tags)
		}
		if !HasTag(tags, TagExpiry) {
			t.Fatalf("%q 应识别为公告节点，实际 %v", n, tags)
		}
	}
}

// TestExtractTagsNoShortCodeFalsePositive 回归：in/us 不该命中普通单词。
func TestExtractTagsNoShortCodeFalsePositive(t *testing.T) {
	cases := map[string]string{
		"in":              "普通单词 in 不应识别为印度",
		"BGP-Line":        "BGP 不应识别出地区",
		"Premium-Node-01": "无地区信息",
	}
	for name, reason := range cases {
		tags := ExtractTags(name, "ss")
		if HasTagPrefix(tags, "region:") {
			t.Fatalf("%q 误报地区（%s），实际 %v", name, reason, tags)
		}
	}
}

func TestExtractTagsInfoNode(t *testing.T) {
	info := []string{
		"剩余流量：878.1 GB",
		"距离下次重置剩余：22 天",
		"套餐到期：2026-12-12",
	}
	for _, n := range info {
		if !HasTag(ExtractTags(n, "vless"), TagExpiry) {
			t.Fatalf("%q 应带 expiry 标签", n)
		}
	}
}

func TestExtractTagsRateFormats(t *testing.T) {
	cases := map[string]string{
		"节点0.1x":  "rate:0.1",
		"节点1x":    "rate:1",
		"节点2.5x":  "rate:2.5",
		"节点0.01x": "rate:0.01",
	}
	for name, want := range cases {
		if !HasTag(ExtractTags(name, "ss"), want) {
			t.Fatalf("%q 应识别 %s，实际 %v", name, want, ExtractTags(name, "ss"))
		}
	}
}

func TestExtractTagsEmpty(t *testing.T) {
	if got := ExtractTags("", "ss"); len(got) != 0 {
		t.Fatalf("空名应返回空标签，得到 %v", got)
	}
}

func TestHasTagHelpers(t *testing.T) {
	tags := []string{"region:HK", "rate:0.1"}
	if !HasTag(tags, "region:HK") {
		t.Fatal("HasTag 失败")
	}
	if HasTag(tags, "region:JP") {
		t.Fatal("HasTag 误报")
	}
	if !HasTagPrefix(tags, "region:") {
		t.Fatal("HasTagPrefix 失败")
	}
	if HasTagPrefix(tags, "proto:") {
		t.Fatal("HasTagPrefix 误报")
	}
}

func TestTagConstantsDistinct(t *testing.T) {
	all := []string{TagRegion, TagRate, TagStream, TagDedicated, TagBGP, TagHighSpeed, TagExpiry}
	seen := map[string]bool{}
	for _, tag := range all {
		if seen[tag] {
			t.Fatalf("标签常量重复: %s", tag)
		}
		seen[tag] = true
		if strings.TrimSpace(tag) != tag {
			t.Fatalf("标签常量有空白: %q", tag)
		}
	}
}

// TestInfoNodeNoFalsePositive 回归：真实节点名不能被当成公告节点。
//
// 踩过两次：一次是 "GB"（吉字节）被当成英国，
// 一次是加了"线路"关键词后把 "香港BGP线路01" 判成公告。
// 公告节点的判定宁严勿宽——漏判的代价是一个垃圾节点，
// 误判的代价是整批真实节点被清掉。
func TestInfoNodeNoFalsePositive(t *testing.T) {
	real := []string{
		"🇭🇰香港高速01|BGP|CMCU",
		"香港BGP线路01",
		"香港01-线路2",
		"日本-优质线路",
		"日本节点01",
		"🇺🇸美国洛杉矶01|流媒体|0.01x",
		"US-LosAngeles-Premium",
	}
	for _, n := range real {
		if InfoNodeName(n) {
			t.Fatalf("误伤真实节点: %q", n)
		}
	}
}

// TestInfoNodeDetectsAnnouncements 公告节点的各类措辞都要能识别。
func TestInfoNodeDetectsAnnouncements(t *testing.T) {
	info := []string{
		"剩余流量：878.1 GB",
		"距离下次重置剩余：22 天",
		"套餐到期：2026-12-12",
		"总量 2 TB",
		"已用 100 GB",
		"上次登录：2026-11-01",
	}
	for _, n := range info {
		if !InfoNodeName(n) {
			t.Fatalf("漏判公告节点: %q", n)
		}
	}
}
