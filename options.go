// Package mihomopool 以独立进程方式管理 mihomo 内核，并把它的多个出站节点
// 暴露成互不干扰的本地代理端口，供异步高并发请求按节点分发。
//
// 设计要点（与"单端口 + 全局 selector 切换"的做法相反）：
//   - 一个内核进程、一份配置，但每个节点在配置里各占一条 listeners 条目，
//     通过 listeners[n].proxy 绑定到指定出站。N 个节点 = N 个本地端口，
//     请求 A 走节点 1 时不会被请求 B 切换节点影响。
//   - 订阅交给 mihomo 自己的 proxy-providers 定时拉取，模块只生成配置骨架。
//   - 控制面走 external-controller REST，支持 secret、超时、重试。
//
// 本包不依赖任何上层业务仓库，跨 Windows / macOS / Linux。
package mihomopool

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"
)

// ProxyMode 是节点经本地端口暴露时使用的代理协议。
type ProxyMode string

const (
	// ModeMixed 同时接受 HTTP 与 SOCKS5（mihomo 的 mixed 类型）。
	ModeMixed ProxyMode = "mixed"
	// ModeSocks 仅 SOCKS5。
	ModeSocks ProxyMode = "socks"
	// ModeHTTP 仅 HTTP。
	ModeHTTP ProxyMode = "http"
)

// Options 是模块的全部可调项。零值配合 DefaultOptions 使用。
type Options struct {
	// Binary 是 mihomo 可执行文件的绝对路径。为空时按搜索链查找。
	Binary string

	// SearchPaths 是 Binary 为空时的候选路径列表，按序检查。
	// 每项可以是绝对路径，也可以是 PATH 里能直接命中的名字。
	// 为空时使用内置搜索链（见 defaultSearchPaths）。
	SearchPaths []string

	// WorkDir 是 mihomo 的工作目录（-d），配置、缓存、日志都落在这里。
	// 为空时落到 os.UserCacheDir()/mihomo-pool。
	WorkDir string

	// ConfigFile 是生成的配置文件名（相对 WorkDir）。默认 config.yaml。
	ConfigFile string

	// SocksPort 是默认共享入站端口（mixed-port），走配置里的规则分流。
	// 默认 17890。
	SocksPort int

	// APIPort 是 external-controller 端口。默认 17891。
	APIPort int

	// APIHost 是 external-controller 监听地址。默认 127.0.0.1。
	APIHost string

	// Secret 是控制面 Bearer 口令。为空时不鉴权。
	Secret string

	// BasePort 是每节点独立端口的起点。默认 17900。
	// 第 i 个节点用 BasePort+i。必须保证 BasePort 到 BasePort+N-1 空闲。
	BasePort int

	// Mode 决定每节点端口暴露的协议。默认 ModeMixed。
	Mode ProxyMode

	// AllowLAN 是否允许局域网访问各端口。默认 false（仅回环）。
	AllowLAN bool

	// LogLevel 是内核日志级别：silent/error/warning/info/debug。默认 warning。
	LogLevel string

	// APIRequestTimeout 是单次控制面请求超时。默认 5s。
	APIRequestTimeout time.Duration

	// APIRetries 是控制面请求的失败重试次数（不含首次）。默认 2。
	// 0 表示使用默认值；负数（如 -1）表示禁用重试。
	APIRetries int

	// StartTimeout 是等待内核端口就绪的上限。默认 20s。
	StartTimeout time.Duration

	// StopTimeout 是优雅退出的等待上限，超时后强杀。默认 8s。
	StopTimeout time.Duration

	// MaxRestarts 是内核异常退出后的自动重启次数上限（0 表示不自动重启）。默认 3。
	MaxRestarts int

	// RestartBackoff 是重启退避基数，第 n 次等待 n*Backoff。默认 2s。
	RestartBackoff time.Duration

	// Env 是额外的环境变量，形如 KEY=VALUE。为空时继承当前进程环境。
	Env []string

	// SkipSharedProbe 为 true 时就绪判定只看控制面端口，不看共享 SOCKS 端口。
	// 用于共享端口被外部占用、只想用节点独立端口的场景。
	SkipSharedProbe bool
}

// DefaultOptions 返回带默认值的选项。
func DefaultOptions() Options {
	return Options{
		ConfigFile:        "config.yaml",
		SocksPort:         17890,
		APIPort:           17891,
		APIHost:           "127.0.0.1",
		BasePort:          17900,
		Mode:              ModeMixed,
		LogLevel:          "warning",
		APIRequestTimeout: 5 * time.Second,
		APIRetries:        2,
		StartTimeout:      20 * time.Second,
		StopTimeout:       8 * time.Second,
		MaxRestarts:       3,
		RestartBackoff:    2 * time.Second,
	}
}

// normalize 补齐零值并做一致性校验。
func (o *Options) normalize() error {
	d := DefaultOptions()
	if o.ConfigFile == "" {
		o.ConfigFile = d.ConfigFile
	}
	if o.SocksPort == 0 {
		o.SocksPort = d.SocksPort
	}
	if o.APIPort == 0 {
		o.APIPort = d.APIPort
	}
	if o.APIHost == "" {
		o.APIHost = d.APIHost
	}
	if o.BasePort == 0 {
		o.BasePort = d.BasePort
	}
	if o.Mode == "" {
		o.Mode = d.Mode
	}
	if o.LogLevel == "" {
		o.LogLevel = d.LogLevel
	}
	if o.APIRequestTimeout <= 0 {
		o.APIRequestTimeout = d.APIRequestTimeout
	}
	if o.APIRetries < 0 {
		o.APIRetries = 0
	} else if o.APIRetries == 0 {
		o.APIRetries = d.APIRetries
	}
	if o.StartTimeout <= 0 {
		o.StartTimeout = d.StartTimeout
	}
	if o.StopTimeout <= 0 {
		o.StopTimeout = d.StopTimeout
	}
	if o.RestartBackoff <= 0 {
		o.RestartBackoff = d.RestartBackoff
	}
	if o.MaxRestarts < 0 {
		o.MaxRestarts = 0
	}

	switch o.Mode {
	case ModeMixed, ModeSocks, ModeHTTP:
	default:
		return fmt.Errorf("mihomo-pool: 未知 Mode %q（可用 mixed/socks/http）", o.Mode)
	}

	for name, port := range map[string]int{
		"SocksPort": o.SocksPort,
		"APIPort":   o.APIPort,
		"BasePort":  o.BasePort,
	} {
		if port <= 0 || port > 65535 {
			return fmt.Errorf("mihomo-pool: %s=%d 越界（1-65535）", name, port)
		}
	}
	if o.SocksPort == o.APIPort {
		return fmt.Errorf("mihomo-pool: SocksPort 与 APIPort 不能相同（都是 %d）", o.SocksPort)
	}
	if o.BasePort == o.SocksPort || o.BasePort == o.APIPort {
		return fmt.Errorf("mihomo-pool: BasePort 与 SocksPort/APIPort 冲突（%d）", o.BasePort)
	}
	if o.WorkDir == "" {
		o.WorkDir = defaultWorkDir()
	}
	abs, err := filepath.Abs(o.WorkDir)
	if err != nil {
		return fmt.Errorf("mihomo-pool: 解析 WorkDir 失败: %w", err)
	}
	o.WorkDir = abs
	return nil
}

// defaultWorkDir 跨平台给出一个可写的数据目录。
func defaultWorkDir() string {
	if d, err := os.UserCacheDir(); err == nil && d != "" {
		return filepath.Join(d, "mihomo-pool")
	}
	if runtime.GOOS == "windows" {
		if d := os.Getenv("LOCALAPPDATA"); d != "" {
			return filepath.Join(d, "mihomo-pool")
		}
	}
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "mihomo-pool")
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		return filepath.Join(h, ".cache", "mihomo-pool")
	}
	return filepath.Join(os.TempDir(), "mihomo-pool")
}

// defaultSearchPaths 是 Binary 为空时的搜索链。
//
// 顺序：显式 env 覆盖 → WorkDir 下 bin/ → 可执行文件同目录 → PATH。
// Windows 额外尝试常见安装位置，但仍然以 PATH 优先。
func defaultSearchPaths(o *Options) []string {
	name := "mihomo"
	if runtime.GOOS == "windows" {
		name = "mihomo.exe"
	}
	var out []string
	if v := os.Getenv("MIHOMO_POOL_BINARY"); v != "" {
		out = append(out, v)
	}
	out = append(out,
		filepath.Join(o.WorkDir, "bin", name),
		filepath.Join(o.WorkDir, name),
		name, // 交给 exec.LookPath
	)
	if runtime.GOOS == "windows" {
		for _, base := range []string{os.Getenv("ProgramFiles"), os.Getenv("ProgramFiles(x86)"), os.Getenv("LOCALAPPDATA")} {
			if base == "" {
				continue
			}
			out = append(out,
				filepath.Join(base, "mihomo", name),
				filepath.Join(base, "Clash Verge", "verge-mihomo.exe"),
			)
		}
	}
	return out
}

// ProcessState 是对外暴露的内核状态快照。
type ProcessState string

const (
	StateStopped  ProcessState = "stopped"
	StateStarting ProcessState = "starting"
	StateRunning  ProcessState = "running"
	StateStopping ProcessState = "stopping"
	StateCrashed  ProcessState = "crashed"
)

// exitErrorString 把内核异常退出整理成人能读的一行。
func exitErrorString(code int, sig string) string {
	if sig != "" {
		return "内核被信号终止: " + sig
	}
	return "内核退出码 " + strconv.Itoa(code)
}

// Version 是库的语义化版本，CLI 与 Python 客户端据此做兼容判断。
const Version = "1.0.0"

// KernelStatus 是 Kernel 的状态快照。
type KernelStatus struct {
	State      ProcessState
	PID        int
	StartedAt  time.Time
	LastExit   string
	Restarts   int
	ConfigPath string
	LogTail    []string
	Binary     string
	// Backend 是内核后端名：process（子进程）或 embedded（链接进本进程）。
	Backend string
}
