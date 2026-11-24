//go:build embedded

package mihomopool

import (
	"context"
	"embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/observable"
	mihomoconfig "github.com/metacubex/mihomo/config"
	"github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/hub"
	"github.com/metacubex/mihomo/hub/executor"
	"github.com/metacubex/mihomo/hub/route"
	"github.com/metacubex/mihomo/listener"
	"github.com/metacubex/mihomo/log"

	"github.com/sirupsen/logrus"
)

// 内嵌内核后端（-tags embedded）。
//
// 与子进程后端的核心差异：
//   - 没有 -d 工作目录：geodata（geoip.metadb / GeoSite.dat）必须自己准备，
//     这里用 go:embed 打进二进制，首次启动释放到 WorkDir。
//   - 没有进程可杀：Shutdown 是异步的，必须等它真的释放端口。
//   - 没有崩溃重启：panic 会带走主进程，因此这里加了 recover 兜底。
//
// 启动路径：解析订阅 → 改 RawConfig → ParseRawConfig → ApplyConfig。
// 不能拼接 YAML：订阅里已有 mixed-port/allow-lan/external-controller，
// 追加同名键会让 YAML 直接报 duplicate key。

//go:embed internal/assets/geoip.metadb
//go:embed internal/assets/GeoSite.dat
var geoAssets embed.FS

const (
	geoIPAssetName   = "internal/assets/geoip.metadb"
	geoSiteAssetName = "internal/assets/GeoSite.dat"
)

// embedBackend 实现 backend 接口。
type embedBackend struct {
	opts Options

	mu       sync.Mutex
	running  bool
	startAt  time.Time
	lastExit string
	logs     *ringBuffer
	geoDir   string

	onExit   func(error)
	stopping atomic.Bool

	// prevLogOut 是接管 logrus 之前的输出目标，Stop 时恢复
	prevLogOut io.Writer

	logMu  sync.Mutex
	logSub *observable.Subscription[log.Event]
}

// newBackend 返回当前构建可用的后端。
func newBackend(o Options) (backend, error) {
	return &embedBackend{
		opts: o,
		logs: newRingBuffer(200),
	}, nil
}

// Name 返回后端名。
func (b *embedBackend) Name() string { return "embedded" }

// PID 在嵌入模式下就是本进程。
func (b *embedBackend) PID() int { return os.Getpid() }

// Running 返回运行状态。
func (b *embedBackend) Running() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.running
}

// LastExit 返回最近一次异常描述。
func (b *embedBackend) LastExit() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastExit
}

// LogTail 返回日志尾部。
func (b *embedBackend) LogTail(n int) []string {
	return b.logs.lines(n)
}

// SetOnExit 注册退出回调。嵌入模式下内核不会"退出"，保留接口以便上层统一。
func (b *embedBackend) SetOnExit(fn func(err error)) {
	b.mu.Lock()
	b.onExit = fn
	b.mu.Unlock()
}

// Start 启动内嵌内核。
func (b *embedBackend) Start(ctx context.Context, cfg []byte, cfgPath string) error {
	b.mu.Lock()
	if b.running {
		b.mu.Unlock()
		return nil
	}
	b.stopping.Store(false)
	b.mu.Unlock()

	// geodata 必须先就位：GEOIP/GEOSITE 规则在 ParseRawConfig 阶段就会读它，
	// 缺文件会直接报 "can't download MMDB"。
	geoDir, err := b.prepareGeoData()
	if err != nil {
		return err
	}

	// 内核的路径解析以 HomeDir 为根；不设就落到 $HOME/.config/mihomo，
	// 那里通常没有 MMDB。
	constant.SetHomeDir(geoDir)

	// 关键：mihomo 的 log 包 init() 里是 logrus.SetOutput(os.Stdout)，
	// 内嵌后会直接写进本进程 stdout，把 CLI 的 JSONL 协议污染掉。
	// log 包没有重导出 SetOutput，这里直接操作同一个 logrus 单例改到 stderr。
	// 这是全局状态，会影响宿主进程的 logrus 输出；Stop 时恢复。
	b.prevLogOut = logrus.StandardLogger().Out
	logrus.SetOutput(os.Stderr)

	// 内核日志通过 observable 订阅拿到，不是 Handler 接口
	b.subscribeLogs()

	start := time.Now()
	if err := b.applyConfig(cfg); err != nil {
		b.mu.Lock()
		b.lastExit = err.Error()
		b.mu.Unlock()
		return err
	}

	b.mu.Lock()
	b.running = true
	b.startAt = start
	b.lastExit = ""
	b.mu.Unlock()

	_, _ = fmt.Fprintf(b.logs, "embedded kernel started in %v\n", time.Since(start))
	return nil
}

// applyConfig 解析并应用配置，带 panic 恢复。
//
// mihomo 内部有大量全局状态初始化，配置异常时可能 panic 而不是返回错误。
// 嵌入模式下 panic 会带走整个进程，所以这里必须兜住。
func (b *embedBackend) applyConfig(cfg []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("mihomo-pool: 内核初始化 panic: %v", r)
		}
	}()

	raw, err := mihomoconfig.UnmarshalRawConfig(cfg)
	if err != nil {
		return fmt.Errorf("mihomo-pool: 解析内核配置失败: %w", err)
	}

	// 强制覆盖端口与控制器：订阅里可能自带，必须由我们决定
	raw.MixedPort = b.opts.SocksPort
	raw.AllowLan = b.opts.AllowLAN
	raw.ExternalController = fmt.Sprintf("%s:%d", b.opts.APIHost, b.opts.APIPort)
	raw.Secret = b.opts.Secret
	if b.opts.LogLevel != "" {
		// mihomo 的 LogLevel 是枚举，不是字符串
		if lvl, ok := log.LogLevelMapping[strings.ToLower(b.opts.LogLevel)]; ok {
			raw.LogLevel = lvl
		}
	}

	parsed, err := mihomoconfig.ParseRawConfig(raw)
	if err != nil {
		return fmt.Errorf("mihomo-pool: 构建内核配置失败: %w", err)
	}

	// 自检：端口必须是我们设的值。mihomo 有多层配置合并，
	// 静默回落到随机端口会让上层探测永远等不到就绪。
	if parsed.General.MixedPort != b.opts.SocksPort {
		return fmt.Errorf("mihomo-pool: 内核端口未生效（期望 %d，实际 %d）",
			b.opts.SocksPort, parsed.General.MixedPort)
	}

	// 必须走 hub.ApplyConfig 而不是 executor.ApplyConfig：
	// 前者会额外调 applyRoute() 启动 external-controller，
	// 只调 executor 的话控制面端口永远不监听，上层等就绪会一直超时。
	hub.ApplyConfig(parsed)
	return nil
}

// prepareGeoData 把内嵌的 geodata 释放到 WorkDir，返回该目录。
//
// 按内容长度校验，避免每次启动重复写 12MB。
func (b *embedBackend) prepareGeoData() (string, error) {
	dir := filepath.Join(b.opts.WorkDir, "geodata")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("mihomo-pool: 创建 geodata 目录失败: %w", err)
	}

	for _, item := range []struct{ asset, name string }{
		{geoIPAssetName, "geoip.metadb"},
		{geoSiteAssetName, "GeoSite.dat"},
	} {
		data, err := geoAssets.ReadFile(item.asset)
		if err != nil {
			return "", fmt.Errorf("mihomo-pool: 读取内嵌 %s 失败: %w", item.name, err)
		}
		dst := filepath.Join(dir, item.name)

		// 大小一致就跳过（geodata 是只读资源，内容不会被人改）
		if st, err := os.Stat(dst); err == nil && st.Size() == int64(len(data)) {
			continue
		}
		tmp := dst + ".tmp"
		if err := os.WriteFile(tmp, data, 0o644); err != nil {
			return "", fmt.Errorf("mihomo-pool: 写出 %s 失败: %w", item.name, err)
		}
		if err := os.Rename(tmp, dst); err != nil {
			_ = os.Remove(tmp)
			return "", fmt.Errorf("mihomo-pool: 落盘 %s 失败: %w", item.name, err)
		}
	}

	b.mu.Lock()
	b.geoDir = dir
	b.mu.Unlock()
	return dir, nil
}

// subscribeLogs 订阅内核日志并转发到 ring buffer。
//
// mihomo 的日志是 observable 模式（Subscribe 返回订阅句柄），
// 不是 slog.Handler 那种接口，所以这里必须用订阅。
func (b *embedBackend) subscribeLogs() {
	b.logMu.Lock()
	defer b.logMu.Unlock()
	if b.logSub != nil {
		return
	}
	sub := log.Subscribe()
	b.logSub = &sub
	go func() {
		for ev := range sub {
			_, _ = fmt.Fprintf(b.logs, "[%s] %s\n", ev.LogLevel.String(), ev.Payload)
		}
	}()
}

// Stop 关闭内嵌内核。
//
// executor.Shutdown 是异步的：它内部会关闭监听、等待连接回收。
// 这里额外轮询端口，确保 Stop 返回时端口真的空了——否则上层立刻重启
// 会撞 "address already in use"。
func (b *embedBackend) Stop(ctx context.Context) error {
	b.mu.Lock()
	if !b.running {
		b.mu.Unlock()
		return nil
	}
	b.stopping.Store(true)
	b.mu.Unlock()

	func() {
		defer func() {
			if r := recover(); r != nil {
				_, _ = fmt.Fprintf(b.logs, "shutdown panic: %v\n", r)
			}
		}()
		// executor.Shutdown 只关 TUN（listener.Cleanup 内部只调 closeTunListener），
		// HTTP/SOCKS/mixed 监听器不会被释放。必须显式把它们重建为端口 0。
		b.closeInbounds()
		executor.Shutdown()
	}()

	// 等端口真正释放；ctx 取消时提前放弃等待（select 里的 break
	// 只退 select 不退循环，所以要用显式标志）。
	deadline := time.Now().Add(b.opts.StopTimeout)
	done := false
	for !done && time.Now().Before(deadline) {
		if !portOpen(b.opts.SocksPort) && !portOpen(b.opts.APIPort) {
			break
		}
		select {
		case <-ctx.Done():
			done = true
		case <-time.After(50 * time.Millisecond):
		}
	}

	b.mu.Lock()
	b.running = false
	if b.prevLogOut != nil {
		logrus.SetOutput(b.prevLogOut)
		b.prevLogOut = nil
	}
	b.mu.Unlock()
	_, _ = fmt.Fprintf(b.logs, "embedded kernel stopped\n")
	return nil
}

// closeInbounds 关闭所有入站监听器。
//
// mihomo 的 executor.Shutdown() 只关 TUN，入站监听器要自己处理：
// ReCreate*(0) 会把已有的监听器关掉且不再重建（端口 0 不监听）。
func (b *embedBackend) closeInbounds() {
	// listeners 段（每节点独立端口）由 inboundListeners 管理：
	// 用空 map + dropOld=true 会把所有旧监听器 Close 并摘除。
	listener.PatchInboundListeners(map[string]constant.InboundListener{}, nil, true)

	// 共享端口是独立的一套（mixedListener 等），要按类型重建为 0
	listener.ReCreateHTTP(0, nil)
	listener.ReCreateSocks(0, nil)
	listener.ReCreateRedir(0, nil)
	listener.ReCreateTProxy(0, nil)
	listener.ReCreateMixed(0, nil)

	// external-controller 由 hub/route 管：传入空 Addr 会先关掉已有 server
	// 并且不再监听（start() 里 len(cfg.Addr) > 0 才 Listen）。
	route.ReCreateServer(&route.Config{})
}

// Reload 用新配置重载，不重启进程。
func (b *embedBackend) Reload(ctx context.Context, cfg []byte, cfgPath string) error {
	b.mu.Lock()
	running := b.running
	b.mu.Unlock()
	if !running {
		return errBackendUnavailable
	}
	if err := b.applyConfig(cfg); err != nil {
		b.mu.Lock()
		b.lastExit = err.Error()
		b.mu.Unlock()
		return err
	}
	_, _ = b.logs.Write([]byte("embedded kernel reloaded\n"))
	return nil
}
