package main

import (
	"encoding/base64"
	"flag"
	"os"
	"strings"
	"testing"
)

func TestParseHeaderFlag(t *testing.T) {
	h := parseHeaderFlag("A: 1;B: 2\nC: 3")
	want := map[string]string{"A": "1", "B": "2", "C": "3"}
	if len(h) != len(want) {
		t.Fatalf("期望 %d 个头，得到 %v", len(want), h)
	}
	for k, v := range want {
		if h[k] != v {
			t.Fatalf("%s 应为 %q，得到 %q", k, v, h[k])
		}
	}

	if len(parseHeaderFlag("")) != 0 {
		t.Fatal("空串应得空 map")
	}
	if len(parseHeaderFlag("没有冒号")) != 0 {
		t.Fatal("无冒号项应被丢弃")
	}
	// 值里含冒号要保留
	h2 := parseHeaderFlag("Referer: https://a.b/c?d=1")
	if h2["Referer"] != "https://a.b/c?d=1" {
		t.Fatalf("值含冒号被截断: %q", h2["Referer"])
	}
}

func TestSplitCSVFlag(t *testing.T) {
	got := splitCSVFlag("hk, us ,, jp")
	want := []string{"hk", "us", "jp"}
	if len(got) != len(want) {
		t.Fatalf("期望 %v，得到 %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("第 %d 项应为 %q，得到 %q", i, want[i], got[i])
		}
	}
	if splitCSVFlag("") != nil {
		t.Fatal("空串应返回 nil")
	}
}

func TestSplitFilter(t *testing.T) {
	f := splitFilter("hk,us", "ss,vmess", "prefix-")
	if len(f.Groups) != 2 || f.Groups[0] != "hk" || f.Groups[1] != "us" {
		t.Fatalf("groups 不对: %v", f.Groups)
	}
	if len(f.Types) != 2 || f.Types[1] != "vmess" {
		t.Fatalf("types 不对: %v", f.Types)
	}
	if f.NamePrefix != "prefix-" {
		t.Fatalf("prefix 不对: %q", f.NamePrefix)
	}
}

func TestParseProviders(t *testing.T) {
	got := parseProviders([]string{"airport=https://x.com/sub|3600", "https://y.com/s", ""})
	if len(got) != 2 {
		t.Fatalf("期望 2 个 provider，得到 %d", len(got))
	}
	if got[0].Name != "airport" || got[0].URL != "https://x.com/sub" || got[0].IntervalSec != 3600 {
		t.Fatalf("第一个 provider 不对: %+v", got[0])
	}
	if got[1].Name != "provider2" || got[1].URL != "https://y.com/s" {
		t.Fatalf("第二个 provider 不对: %+v", got[1])
	}
}

func TestRequestMsgToLibraryRequest(t *testing.T) {
	body := "hello"
	m := &requestMsg{
		URL:     "https://example.com/a",
		Method:  "post",
		Header:  map[string]string{"X-Test": "1"},
		Body:    &body,
		Tag:     "t1",
		HashKey: "k",
	}
	req, err := m.toLibraryRequest()
	if err != nil {
		t.Fatalf("转换失败: %v", err)
	}
	if req.Method != "POST" {
		t.Fatalf("方法应为大写 POST，得到 %q", req.Method)
	}
	if string(req.Body) != "hello" {
		t.Fatalf("body 不对: %q", req.Body)
	}
	if req.Timeout.Seconds() != 20 {
		t.Fatalf("默认超时应为 20s，得到 %v", req.Timeout)
	}
	if req.Retries != 1 {
		t.Fatalf("默认重试应为 1，得到 %d", req.Retries)
	}
	if req.Tag != "t1" || req.HashKey != "k" {
		t.Fatalf("tag/hash 丢失: %+v", req)
	}
}

func TestRequestMsgBodyB64(t *testing.T) {
	raw := []byte{0x00, 0x01, 0xff, 0xfe}
	b64 := base64.StdEncoding.EncodeToString(raw)
	m := &requestMsg{URL: "https://e.com", BodyB64: &b64}
	req, err := m.toLibraryRequest()
	if err != nil {
		t.Fatalf("转换失败: %v", err)
	}
	if len(req.Body) != len(raw) {
		t.Fatalf("二进制 body 长度不对: %d", len(req.Body))
	}
	for i := range raw {
		if req.Body[i] != raw[i] {
			t.Fatalf("第 %d 字节不对: %02x", i, req.Body[i])
		}
	}
}

func TestRequestMsgBodyJSON(t *testing.T) {
	m := &requestMsg{
		URL:      "https://e.com",
		BodyJSON: map[string]any{"a": 1, "b": "x"},
	}
	req, err := m.toLibraryRequest()
	if err != nil {
		t.Fatalf("转换失败: %v", err)
	}
	if req.Header["Content-Type"] != "application/json" {
		t.Fatalf("应自动设 Content-Type，得到 %v", req.Header)
	}
	if !strings.Contains(string(req.Body), `"a":1`) {
		t.Fatalf("body_json 序列化不对: %s", req.Body)
	}
}

func TestRequestMsgMissingURL(t *testing.T) {
	if _, err := (&requestMsg{}).toLibraryRequest(); err == nil {
		t.Fatal("缺 url 应报错")
	}
}

func TestRequestMsgFilterMapping(t *testing.T) {
	r := 3
	m := &requestMsg{
		URL:         "https://e.com",
		Retries:     &r,
		Groups:      []string{"hk"},
		Types:       []string{"ss"},
		NamePrefix:  "p-",
		RequirePort: true,
		HealthyOnly: true,
		TimeoutMS:   1500,
	}
	req, err := m.toLibraryRequest()
	if err != nil {
		t.Fatalf("转换失败: %v", err)
	}
	if req.Retries != 3 {
		t.Fatalf("retries 不对: %d", req.Retries)
	}
	if req.Timeout.Milliseconds() != 1500 {
		t.Fatalf("timeout 不对: %v", req.Timeout)
	}
	if !req.Filter.RequirePort || !req.Filter.HealthyOnly || req.Filter.NamePrefix != "p-" {
		t.Fatalf("filter 不对: %+v", req.Filter)
	}
}

func TestDecodeBase64Flexible(t *testing.T) {
	cases := []string{
		base64.StdEncoding.EncodeToString([]byte("abc")),
		base64.RawStdEncoding.EncodeToString([]byte("abc")),
		base64.URLEncoding.EncodeToString([]byte("abc")),
	}
	for _, c := range cases {
		b, err := decodeBase64Flexible(c)
		if err != nil {
			t.Fatalf("%q 解码失败: %v", c, err)
		}
		if string(b) != "abc" {
			t.Fatalf("%q 解码结果不对: %q", c, b)
		}
	}
	// 带换行
	b, err := decodeBase64Flexible("YWJj\n")
	if err != nil || string(b) != "abc" {
		t.Fatalf("带换行解码失败: %v %q", err, b)
	}
}

func TestArgHelpers(t *testing.T) {
	args := map[string]any{
		"s": "x", "n": float64(5), "b": true,
		"arr": []any{"a", "b"},
		"s2":  "only",
	}
	if argString(args, "s") != "x" {
		t.Fatal("argString 失败")
	}
	if argString(args, "missing") != "" {
		t.Fatal("缺失应返回空串")
	}
	if argInt(args, "n", 0) != 5 {
		t.Fatal("argInt 解析 float 失败")
	}
	if argInt(args, "missing", 9) != 9 {
		t.Fatal("argInt 默认值失败")
	}
	if !argBool(args, "b", false) {
		t.Fatal("argBool 失败")
	}
	if got := argSlice(args, "arr"); len(got) != 2 || got[1] != "b" {
		t.Fatalf("argSlice 失败: %v", got)
	}
	if got := argSlice(args, "s2"); len(got) != 1 || got[0] != "only" {
		t.Fatalf("argSlice 单值失败: %v", got)
	}
}

func TestSplitOn(t *testing.T) {
	got := splitOn("a,b;c", ',', ';')
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("splitOn 失败: %v", got)
	}
}

func TestTrimSpaceNL(t *testing.T) {
	if got := trimSpaceNL(" \t abc \r\n"); got != "abc" {
		t.Fatalf("trim 失败: %q", got)
	}
	if got := trimSpaceNL("   "); got != "" {
		t.Fatalf("全空白应为空: %q", got)
	}
}

func TestApplyBatchDefaults(t *testing.T) {
	in := []requestMsg{
		{URL: "https://a", Op: "request"},
		{URL: "https://b"},
	}
	out := applyBatchDefaults(in, 5000, 2)
	if len(out) != 2 {
		t.Fatal("长度不对")
	}
	for _, m := range out {
		if m.TimeoutMS != 5000 {
			t.Fatalf("默认超时未填: %d", m.TimeoutMS)
		}
		if m.Retries == nil || *m.Retries != 2 {
			t.Fatalf("默认重试未填: %v", m.Retries)
		}
		if m.Op == "" {
			t.Fatal("op 未补默认")
		}
	}
}

func TestReadNodesFileParsing(t *testing.T) {
	dir := t.TempDir()
	p := dir + "/nodes.json"
	content := `[
	  {"name":"hk","type":"ss","server":"1.1.1.1","port":8388,"group":"hk","raw":{"cipher":"aes-128-gcm","password":"p"}},
	  {"name":"us","type":"trojan","server":"2.2.2.2","port":443}
	]`
	if err := writeTestFile(p, content); err != nil {
		t.Fatal(err)
	}
	nodes, err := readNodesFile(p)
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("期望 2 个节点，得到 %d", len(nodes))
	}
	if nodes[0].Name != "hk" || nodes[0].Port != 8388 || nodes[0].Group != "hk" {
		t.Fatalf("第一个节点不对: %+v", nodes[0])
	}
	if nodes[0].Raw["cipher"] != "aes-128-gcm" {
		t.Fatalf("raw 未保留: %+v", nodes[0].Raw)
	}
}

func TestReadPIDFilePlainNumber(t *testing.T) {
	p := t.TempDir() + "/pid"
	if err := writeTestFile(p, "12345\n"); err != nil {
		t.Fatal(err)
	}
	pid, err := readPIDFile(p)
	if err != nil || pid != 12345 {
		t.Fatalf("读 PID 失败: %d %v", pid, err)
	}
}

func TestReadPIDFileJSON(t *testing.T) {
	p := t.TempDir() + "/ready.json"
	if err := writeTestFile(p, `{"ok":true,"pid":999,"kernel_pid":1000}`); err != nil {
		t.Fatal(err)
	}
	pid, err := readPIDFile(p)
	if err != nil || pid != 999 {
		t.Fatalf("读 JSON PID 失败: %d %v", pid, err)
	}
}

func TestReadPIDFileInvalid(t *testing.T) {
	p := t.TempDir() + "/bad"
	if err := writeTestFile(p, "not-a-pid"); err != nil {
		t.Fatal(err)
	}
	if _, err := readPIDFile(p); err == nil {
		t.Fatal("非法内容应报错")
	}
}

// writeTestFile 写测试文件。
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o644)
}

func TestSplitGlobalArgsBeforeSubcommand(t *testing.T) {
	// 全局参数写在子命令之前：Python 客户端就是这个顺序
	cmd, args := splitGlobalArgs([]string{"-bin", "/x/mihomo", "-dir", "/tmp/w", "serve", "-workers", "8"})
	if cmd != "serve" {
		t.Fatalf("子命令应为 serve，得到 %q", cmd)
	}
	joined := strings.Join(args, " ")
	for _, want := range []string{"-bin /x/mihomo", "-dir /tmp/w", "-workers 8"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args 缺少 %q，得到 %q", want, joined)
		}
	}
}

func TestSplitGlobalArgsSubcommandFirst(t *testing.T) {
	cmd, args := splitGlobalArgs([]string{"call", "-bin", "/x", "-timeout", "9000", "https://e.com"})
	if cmd != "call" {
		t.Fatalf("子命令应为 call，得到 %q", cmd)
	}
	if args[len(args)-1] != "https://e.com" {
		t.Fatalf("URL 应保留在末尾，得到 %v", args)
	}
	if args[0] != "-bin" {
		t.Fatalf("call 自带的参数应原样保留，得到 %v", args)
	}
}

func TestSplitGlobalArgsEqualsForm(t *testing.T) {
	cmd, args := splitGlobalArgs([]string{"-bin=/usr/bin/mihomo", "-allow-lan", "serve"})
	if cmd != "serve" {
		t.Fatalf("子命令应为 serve，得到 %q", cmd)
	}
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "-bin=/usr/bin/mihomo") || !strings.Contains(joined, "-allow-lan") {
		t.Fatalf("args 不对: %v", args)
	}
}

func TestApplyBatchDefaultsUniqueIDs(t *testing.T) {
	// 批量输入通常不带 id：必须自动生成，否则回填时互相覆盖
	in := []requestMsg{
		{URL: "https://a"},
		{URL: "https://b"},
		{URL: "https://c", ID: "dup"},
		{URL: "https://d", ID: "dup"},
	}
	out := applyBatchDefaults(in, 1000, 1)
	seen := map[string]bool{}
	for _, m := range out {
		if m.ID == "" {
			t.Fatal("id 未生成")
		}
		if seen[m.ID] {
			t.Fatalf("id 重复: %q", m.ID)
		}
		seen[m.ID] = true
	}
	if len(seen) != 4 {
		t.Fatalf("应有 4 个唯一 id，得到 %d", len(seen))
	}
}

func TestProbeCommandsAcceptSub(t *testing.T) {
	// nodes / health 必须接受 -sub 与 -nodes，否则"给订阅直接查"
	// 这个最基本的用法就断了（之前只支持连已运行内核）。
	for _, cmd := range []string{"nodes", "health"} {
		fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
		var cf commonFlags
		cf.bind(fs)
		sub := fs.String("sub", "", "")
		nodesFile := fs.String("nodes", "", "")
		if cmd == "nodes" {
			fs.Bool("pretty", false, "")
			fs.Bool("refresh", true, "")
			fs.Bool("keep-info-nodes", false, "")
			fs.Bool("keep-duplicates", false, "")
		} else {
			fs.Int("concurrency", 16, "")
			fs.Bool("pretty", false, "")
		}
		args := []string{"-sub", "https://e.com/sub", "-nodes", "/tmp/n.json"}
		if err := fs.Parse(args); err != nil {
			t.Fatalf("%s 解析 -sub 失败: %v", cmd, err)
		}
		if *sub != "https://e.com/sub" {
			t.Fatalf("%s 的 -sub 未生效: %q", cmd, *sub)
		}
		if *nodesFile != "/tmp/n.json" {
			t.Fatalf("%s 的 -nodes 未生效: %q", cmd, *nodesFile)
		}
	}
}
