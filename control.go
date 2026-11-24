package mihomopool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// external-controller REST 客户端。
//
// 覆盖的端点（mihomo v1.19.x）：
//   GET  /version
//   GET  /configs                    当前运行配置
//   PUT  /configs  {"path":...}      重载指定配置文件（Windows 下的重载手段）
//   PATCH /configs {"mode":...}      改运行模式
//   GET  /proxies                    全部节点与分组
//   GET  /proxies/{name}/delay       单节点测速（内核内部发起，不出本机）
//   GET  /group/{name}/delay         整组测速
//   PUT  /proxies/{group} {"name":..} 切换 selector 选中项
//   GET  /providers/proxies          订阅 provider 列表
//   PUT  /providers/proxies/{name}   手动触发 provider 刷新

// Control 是控制面客户端。
type Control struct {
	base    string
	secret  string
	client  *http.Client
	retries int
	timeout time.Duration
}

// newControl 按 Options 构造客户端。
func newControl(o Options) *Control {
	tr := &http.Transport{
		Proxy: nil, // 控制面永远直连，不走任何上游代理
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   16,
		IdleConnTimeout:       60 * time.Second,
		ResponseHeaderTimeout: o.APIRequestTimeout,
		DisableCompression:    false,
	}
	return &Control{
		base:    "http://" + net.JoinHostPort(o.APIHost, strconv.Itoa(o.APIPort)),
		secret:  o.Secret,
		client:  &http.Client{Transport: tr, Timeout: o.APIRequestTimeout},
		retries: o.APIRetries,
		timeout: o.APIRequestTimeout,
	}
}

// APIError 是控制面返回的非 2xx。
type APIError struct {
	Method string
	Path   string
	Code   int
	Body   string
}

func (e *APIError) Error() string {
	body := strings.TrimSpace(e.Body)
	if len(body) > 300 {
		body = body[:300] + "..."
	}
	return fmt.Sprintf("mihomo 控制面 %s %s 返回 %d: %s", e.Method, e.Path, e.Code, body)
}

// Version 返回内核版本。
func (c *Control) Version(ctx context.Context) (string, error) {
	var out struct {
		Version string `json:"version"`
		Meta    bool   `json:"meta"`
	}
	if err := c.do(ctx, http.MethodGet, "/version", nil, &out, false); err != nil {
		return "", err
	}
	if out.Version == "" {
		return "", errors.New("mihomo-pool: /version 未返回版本号")
	}
	return out.Version, nil
}

// Pinger 检查控制面是否可达。
func (c *Control) Pinger(ctx context.Context) error {
	_, err := c.Version(ctx)
	return err
}

// ProxyItem 是 /proxies 里的一个条目。
type ProxyItem struct {
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	Alive        bool     `json:"alive"`
	Now          string   `json:"now"`
	All          []string `json:"all"`
	ProviderName string   `json:"provider-name"`
	History      []struct {
		Time  time.Time `json:"time"`
		Delay int64     `json:"delay"`
	} `json:"history"`
	UDP bool `json:"udp"`
}

// builtinOutboundNames 是 mihomo 内置出站的名字。
//
// 它们会出现在 /proxies 里，但不是可用节点：没有远端服务器，也没有独立端口，
// 派发给它们等于把请求交给规则引擎，出口不可控。
const builtinOutboundNames = "DIRECT|REJECT|REJECT-DROP|PASS|PASS-RULE|COMPATIBLE|GLOBAL"

// isBuiltinOutbound 判断名字是否为内置出站。
func isBuiltinOutbound(name string) bool {
	up := strings.ToUpper(strings.TrimSpace(name))
	for _, b := range strings.Split(builtinOutboundNames, "|") {
		if up == b {
			return true
		}
	}
	return false
}

// IsGroup 判断该条目是否为策略组/选择器/内置出站而非真实节点。
//
// 实测（mihomo v1.19.34）需要同时排除：
//
//	Selector / URLTest / Fallback / LoadBalance / Relay   策略组
//	Direct / Reject / RejectDrop / Pass / PassRule        内置出站规则
//	Compatible / Dns                                      占位出站
//
// 注意内置名是 "PASS-RULE"、类型是 "PassRule"（无连字符）。早期只列了 Pass，
// 结果 PASS-RULE 被当成节点派发出去，请求走了不可控的规则出口。
func (p ProxyItem) IsGroup() bool {
	if len(p.All) > 0 {
		return true
	}
	if isBuiltinOutbound(p.Name) {
		return true
	}
	switch p.Type {
	case "Selector", "URLTest", "Fallback", "LoadBalance", "Relay",
		"Direct", "Reject", "RejectDrop", "Pass", "PassRule",
		"Compatible", "Dns":
		return true
	}
	return false
}

// Delay 返回最近一次测速的历史延迟（毫秒），没有历史返回 -1。
func (p ProxyItem) Delay() int64 {
	if len(p.History) == 0 {
		return -1
	}
	return p.History[len(p.History)-1].Delay
}

// Proxies 返回全部节点与分组。
func (c *Control) Proxies(ctx context.Context) (map[string]ProxyItem, error) {
	var wrap struct {
		Proxies map[string]ProxyItem `json:"proxies"`
	}
	if err := c.do(ctx, http.MethodGet, "/proxies", nil, &wrap, true); err != nil {
		return nil, err
	}
	if wrap.Proxies == nil {
		return map[string]ProxyItem{}, nil
	}
	return wrap.Proxies, nil
}

// Nodes 只返回真实出站节点（排除策略组与内置类型）。
func (c *Control) Nodes(ctx context.Context) ([]ProxyItem, error) {
	all, err := c.Proxies(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]ProxyItem, 0, len(all))
	for _, p := range all {
		if p.IsGroup() {
			continue
		}
		out = append(out, p)
	}
	sortProxyItems(out)
	return out, nil
}

// Delay 对单个节点发起测速，返回毫秒延迟。
//
// 测速由内核自己发起，出口就是该节点，不消耗本地连接数。
func (c *Control) Delay(ctx context.Context, name, testURL string, timeout time.Duration) (int64, error) {
	if strings.TrimSpace(testURL) == "" {
		testURL = DefaultDelayTestURL
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	q := url.Values{}
	q.Set("url", testURL)
	q.Set("timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	path := "/proxies/" + url.PathEscape(name) + "/delay?" + q.Encode()

	var out struct {
		Delay   int64  `json:"delay"`
		Message string `json:"message"`
	}
	if err := c.do(ctx, http.MethodGet, path, nil, &out, false); err != nil {
		return 0, err
	}
	if out.Delay <= 0 {
		if out.Message != "" {
			return 0, fmt.Errorf("mihomo-pool: 节点 %s 测速失败: %s", name, out.Message)
		}
		return 0, fmt.Errorf("mihomo-pool: 节点 %s 测速未返回延迟", name)
	}
	return out.Delay, nil
}

// GroupDelay 对整组发起测速，返回 节点名 → 延迟。
func (c *Control) GroupDelay(ctx context.Context, group, testURL string, timeout time.Duration) (map[string]int64, error) {
	if strings.TrimSpace(testURL) == "" {
		testURL = DefaultDelayTestURL
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	q := url.Values{}
	q.Set("url", testURL)
	q.Set("timeout", strconv.FormatInt(timeout.Milliseconds(), 10))
	path := "/group/" + url.PathEscape(group) + "/delay?" + q.Encode()

	var out map[string]int64
	if err := c.do(ctx, http.MethodGet, path, nil, &out, false); err != nil {
		return nil, err
	}
	return out, nil
}

// SelectNode 把某个 selector 组切到指定节点。
//
// 注意：这是全局状态，会影响所有共用该组的连接。
// 高并发分发请用每节点独立端口（Bindings），不要靠它切节点。
func (c *Control) SelectNode(ctx context.Context, group, node string) error {
	if strings.TrimSpace(group) == "" {
		group = "GLOBAL"
	}
	body := map[string]string{"name": node}
	return c.do(ctx, http.MethodPut, "/proxies/"+url.PathEscape(group), body, nil, false)
}

// SetConfigPath 让内核重载指定配置文件（Windows 下的重载路径）。
func (c *Control) SetConfigPath(ctx context.Context, path string) error {
	body := map[string]string{"path": path}
	return c.do(ctx, http.MethodPut, "/configs", body, nil, false)
}

// RefreshProvider 手动触发一个 provider 刷新。
func (c *Control) RefreshProvider(ctx context.Context, name string) error {
	return c.do(ctx, http.MethodPut, "/providers/proxies/"+url.PathEscape(name), nil, nil, false)
}

// ProviderInfo 是 /providers/proxies 里的一个 provider。
type ProviderInfo struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Vehicle string      `json:"vehicleType"`
	Proxies []ProxyItem `json:"proxies"`
	Update  struct {
		At time.Time `json:"updatedAt"`
	} `json:"-"`
}

// Providers 返回订阅 provider 列表。
func (c *Control) Providers(ctx context.Context) (map[string]ProviderInfo, error) {
	var wrap struct {
		Providers map[string]ProviderInfo `json:"providers"`
	}
	if err := c.do(ctx, http.MethodGet, "/providers/proxies", nil, &wrap, true); err != nil {
		return nil, err
	}
	if wrap.Providers == nil {
		return map[string]ProviderInfo{}, nil
	}
	return wrap.Providers, nil
}

// do 执行一次请求，按需重试。
//
// retry 为 true 时对网络层错误与 5xx 做指数退避重试；
// 4xx（含鉴权失败）立即返回，重试没有意义。
func (c *Control) do(ctx context.Context, method, path string, body any, out any, retry bool) error {
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("mihomo-pool: 序列化请求失败: %w", err)
		}
		payload = b
	}

	attempts := 1
	if retry {
		attempts = c.retries + 1
	}

	var lastErr error
	for i := 0; i < attempts; i++ {
		if i > 0 {
			backoff := time.Duration(i) * 150 * time.Millisecond
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		reqCtx, cancel := context.WithTimeout(ctx, c.timeout)
		req, err := http.NewRequestWithContext(reqCtx, method, c.base+path, bytes.NewReader(payload))
		if err != nil {
			cancel()
			return fmt.Errorf("mihomo-pool: 构造请求失败: %w", err)
		}
		if payload != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if c.secret != "" {
			req.Header.Set("Authorization", "Bearer "+c.secret)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		raw, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		cancel()

		if readErr != nil {
			lastErr = readErr
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			apiErr := &APIError{Method: method, Path: path, Code: resp.StatusCode, Body: string(raw)}
			if resp.StatusCode >= 500 && i < attempts-1 {
				lastErr = apiErr
				continue
			}
			return apiErr
		}

		if out != nil && len(raw) > 0 {
			if err := json.Unmarshal(raw, out); err != nil {
				return fmt.Errorf("mihomo-pool: 解析 %s 响应失败: %w（原文: %s）", path, err, truncate(string(raw), 200))
			}
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("未知错误")
	}
	return fmt.Errorf("mihomo-pool: 控制面请求 %s %s 失败（已重试 %d 次）: %w", method, path, attempts-1, lastErr)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func sortProxyItems(items []ProxyItem) {
	// 稳定排序：按延迟升序，未测速的排最后
	sort.SliceStable(items, func(i, j int) bool {
		a, b := items[i].Delay(), items[j].Delay()
		if b < 0 {
			return false
		}
		if a < 0 {
			return true
		}
		return a < b
	})
}

// DefaultDelayTestURL 是默认测速地址：极小的 204 响应，不产生业务流量。
const DefaultDelayTestURL = "https://www.gstatic.com/generate_204"
