package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// 协议层：定义所有 op 的请求/响应结构，serve / call / batch 共用同一套。
//
// 兼容策略：
//   - 所有字段名 snake_case
//   - 未知字段忽略（forward compatible）
//   - body 支持三种写法：body（纯文本）、body_b64（二进制安全）、body_json（自动序列化）
//   - 响应里 body 用 b64，避免任意字节破坏 JSON

// requestMsg 是客户端发来的一个请求或控制指令。
type requestMsg struct {
	ID   string         `json:"id"`
	Op   string         `json:"op"`
	Args map[string]any `json:"args"`

	// 直接把 Request 的字段平铺在顶层，省一层嵌套，写起来更顺手
	Method       string            `json:"method"`
	URL          string            `json:"url"`
	Header       map[string]string `json:"header"`
	Headers      map[string]string `json:"headers"` // 别名
	Body         *string           `json:"body"`
	BodyB64      *string           `json:"body_b64"`
	BodyJSON     any               `json:"body_json"`
	TimeoutMS    int               `json:"timeout_ms"`
	Retries      *int              `json:"retries"`
	Tag          string            `json:"tag"`
	HashKey      string            `json:"hash_key"`
	AllowDirect  *bool             `json:"allow_direct"`
	SkipBody     bool              `json:"skip_body"`
	MaxBodyBytes int64             `json:"max_body_bytes"`
	Groups       []string          `json:"groups"`
	Types        []string          `json:"types"`
	NamePrefix   string            `json:"name_prefix"`
	RequirePort  bool              `json:"require_port"`
	HealthyOnly  bool              `json:"healthy_only"`
}

// responseMsg 是回给客户端的一条结果或错误。
type responseMsg struct {
	ID   string `json:"id,omitempty"`
	Op   string `json:"op,omitempty"`
	OK   bool   `json:"ok"`
	Kind string `json:"kind,omitempty"` // resp | err | ack | event | hello

	StatusCode int                 `json:"status,omitempty"`
	Header     map[string][]string `json:"header,omitempty"`
	BodyB64    string              `json:"body_b64,omitempty"`
	BodyLen    int                 `json:"body_len,omitempty"`
	Truncated  bool                `json:"truncated,omitempty"`
	Node       string              `json:"node,omitempty"`
	ProxyURL   string              `json:"proxy_url,omitempty"`
	Attempts   int                 `json:"attempts,omitempty"`
	DurationMS int64               `json:"duration_ms,omitempty"`
	Tag        string              `json:"tag,omitempty"`

	Error string `json:"error,omitempty"`

	// 控制类 op 的返回
	Data map[string]any `json:"data,omitempty"`
}

// toLibraryRequest 把一个协议消息转成库请求。
func (m *requestMsg) toLibraryRequest() (*mihomopool.Request, error) {
	url := strings.TrimSpace(m.URL)
	if url == "" {
		return nil, fmt.Errorf("缺少 url")
	}

	header := map[string]string{}
	for k, v := range m.Header {
		header[k] = v
	}
	for k, v := range m.Headers {
		header[k] = v
	}

	var body []byte
	switch {
	case m.BodyB64 != nil:
		b, err := decodeBase64Flexible(*m.BodyB64)
		if err != nil {
			return nil, fmt.Errorf("body_b64 解码失败: %w", err)
		}
		body = b
	case m.BodyJSON != nil:
		b, err := json.Marshal(m.BodyJSON)
		if err != nil {
			return nil, fmt.Errorf("body_json 序列化失败: %w", err)
		}
		body = b
		if header["Content-Type"] == "" && header["content-type"] == "" {
			header["Content-Type"] = "application/json"
		}
	case m.Body != nil:
		body = []byte(*m.Body)
	}

	timeout := time.Duration(m.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	retries := 1
	if m.Retries != nil {
		retries = *m.Retries
	}
	allowDirect := false
	if m.AllowDirect != nil {
		allowDirect = *m.AllowDirect
	}

	return &mihomopool.Request{
		Method:  strings.ToUpper(strings.TrimSpace(m.Method)),
		URL:     url,
		Header:  header,
		Body:    body,
		Timeout: timeout,
		Retries: retries,
		Filter: mihomopool.NodeFilter{
			Groups:      m.Groups,
			Types:       m.Types,
			NamePrefix:  m.NamePrefix,
			RequirePort: m.RequirePort,
			HealthyOnly: m.HealthyOnly,
		},
		HashKey:      m.HashKey,
		AllowDirect:  allowDirect,
		Tag:          m.Tag,
		SkipBody:     m.SkipBody,
		MaxBodyBytes: m.MaxBodyBytes,
	}, nil
}

// fromLibraryResponse 把库结果转成协议消息。
func fromLibraryResponse(id string, resp *mihomopool.Response) *responseMsg {
	out := &responseMsg{
		ID:         id,
		Op:         "request",
		Kind:       "resp",
		OK:         resp.OK(),
		StatusCode: resp.Status,
		Node:       resp.Node,
		ProxyURL:   resp.ProxyURL,
		Attempts:   resp.Attempts,
		DurationMS: resp.Duration.Milliseconds(),
		Truncated:  resp.Truncated,
		Tag:        resp.Tag,
	}
	if resp.Err != nil {
		out.Kind = "err"
		out.Error = resp.Err.Error()
	}
	if resp.Header != nil {
		h := make(map[string][]string, len(resp.Header))
		for k, v := range resp.Header {
			h[k] = v
		}
		out.Header = h
	}
	if len(resp.Body) > 0 {
		out.BodyB64 = base64.StdEncoding.EncodeToString(resp.Body)
		out.BodyLen = len(resp.Body)
	}
	return out
}

// decodeBase64Flexible 兼容标准/URL 安全、带/不带 padding。
func decodeBase64Flexible(s string) ([]byte, error) {
	trimmed := strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r', ' ', '\t':
			return -1
		}
		return r
	}, s)
	encs := []*base64.Encoding{
		base64.StdEncoding, base64.RawStdEncoding,
		base64.URLEncoding, base64.RawURLEncoding,
	}
	var lastErr error
	for _, e := range encs {
		if b, err := e.DecodeString(trimmed); err == nil {
			return b, nil
		} else {
			lastErr = err
		}
	}
	return nil, lastErr
}

// argString 从 args 里安全取字符串。
func argString(args map[string]any, key string) string {
	if args == nil {
		return ""
	}
	if v, ok := args[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
		return fmt.Sprint(v)
	}
	return ""
}

// argInt 从 args 里安全取整数。
func argInt(args map[string]any, key string, def int) int {
	if args == nil {
		return def
	}
	v, ok := args[key]
	if !ok {
		return def
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case string:
		if n, err := strconv.Atoi(x); err == nil {
			return n
		}
	case json.Number:
		if n, err := x.Int64(); err == nil {
			return int(n)
		}
	}
	return def
}

// argBool 从 args 里安全取布尔。
func argBool(args map[string]any, key string, def bool) bool {
	if args == nil {
		return def
	}
	v, ok := args[key]
	if !ok {
		return def
	}
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return x == "true" || x == "1" || x == "yes"
	case float64:
		return x != 0
	}
	return def
}

// argSlice 从 args 里取字符串数组。
func argSlice(args map[string]any, key string) []string {
	if args == nil {
		return nil
	}
	v, ok := args[key]
	if !ok {
		return nil
	}
	arr, ok := v.([]any)
	if !ok {
		if s, ok := v.(string); ok && s != "" {
			return []string{s}
		}
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		out = append(out, fmt.Sprint(item))
	}
	return out
}

// nodeFilterFromArgs 从 args 构造筛选条件。
func nodeFilterFromArgs(args map[string]any) mihomopool.NodeFilter {
	return mihomopool.NodeFilter{
		Groups:      argSlice(args, "groups"),
		Types:       argSlice(args, "types"),
		NamePrefix:  argString(args, "name_prefix"),
		RequirePort: argBool(args, "require_port", false),
		HealthyOnly: argBool(args, "healthy_only", false),
	}
}

// nodesToData 把节点列表转成可序列化结构。
func nodesToData(nodes []*mihomopool.Node) []map[string]any {
	out := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, map[string]any{
			"name":      n.Name,
			"type":      n.Type,
			"group":     n.Group,
			"provider":  n.Provider,
			"proxy_url": n.ProxyURL,
			"port":      n.Port,
			"delay_ms":  n.DelayMS,
			"alive":     n.Alive,
			"healthy":   n.Healthy(),
			"score":     n.Score(),
			"inflight":  n.Inflight(),
		})
	}
	return out
}

// ctxWithTimeout 给控制类 op 用的超时包装。
func ctxWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		d = 30 * time.Second
	}
	return context.WithTimeout(parent, d)
}
