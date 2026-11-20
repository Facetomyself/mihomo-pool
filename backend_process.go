//go:build !embedded

package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 子进程内核后端（默认，不带 -tags embedded）。
//
// 把 mihomo 当独立进程跑：崩溃不会带走主进程，内核升级只需换二进制。
// 代价是启动要 100ms~2s，且需要用户自己装内核。

// processBackend 实现 backend 接口。
type processBackend struct {
	opts Options

	mu       sync.Mutex
	cmd      *exec.Cmd
	bin      string
	logs     *ringBuffer
	lastExit string
	onExit   func(error)

	done    chan struct{}
	closing atomic.Bool
	logPath string
}

// newBackend 返回当前构建可用的后端。
func newBackend(o Options) (backend, error) {
	return &processBackend{
		opts: o,
		logs: newRingBuffer(200),
	}, nil
}

// Name 返回后端名。
func (b *processBackend) Name() string { return "process" }

// PID 返回内核进程 pid，未运行返回 0。
func (b *processBackend) PID() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cmd == nil || b.cmd.Process == nil {
		return 0
	}
	return b.cmd.Process.Pid
}

// Running 判断内核是否在跑。
func (b *processBackend) Running() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cmd != nil && b.cmd.Process != nil
}

// LastExit 返回最近一次异常退出描述。
func (b *processBackend) LastExit() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastExit
}

// LogTail 返回日志尾部。
func (b *processBackend) LogTail(n int) []string {
	b.mu.Lock()
	path := b.logPath
	b.mu.Unlock()
	if path == "" {
		return b.logs.lines(n)
	}
	if lines := tailFile(path, n); len(lines) > 0 {
		return lines
	}
	return b.logs.lines(n)
}

// SetOnExit 注册退出回调。
func (b *processBackend) SetOnExit(fn func(err error)) {
	b.mu.Lock()
	b.onExit = fn
	b.mu.Unlock()
}

// resolveBinary 按搜索链找可执行文件。
func (b *processBackend) resolveBinary() (string, error) {
	if p := trimSpace(b.opts.Binary); p != "" {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p, nil
		}
		return "", fmt.Errorf("mihomo-pool: Binary 指定为 %q 但文件不存在", b.opts.Binary)
	}
	cands := b.opts.SearchPaths
	if len(cands) == 0 {
		cands = defaultSearchPaths(&b.opts)
	}
	for _, c := range cands {
		c = trimSpace(c)
		if c == "" {
			continue
		}
		if filepath.IsAbs(c) || containsAny(c, `/\`) {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c, nil
			}
			continue
		}
		if p, err := exec.LookPath(c); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("mihomo-pool: 未找到 mihomo 内核，已尝试 %d 个位置（把可执行文件放到 %s 或设置 Options.Binary；或改用 -tags embedded 构建，不需要外部内核）",
		len(cands), filepath.Join(b.opts.WorkDir, "bin"))
}

// Start 拉起内核进程。
func (b *processBackend) Start(ctx context.Context, cfg []byte, cfgPath string) error {
	bin, err := b.resolveBinary()
	if err != nil {
		return err
	}

	logPath := filepath.Join(b.opts.WorkDir, "mihomo.log")
	if err := os.MkdirAll(b.opts.WorkDir, 0o755); err != nil {
		return fmt.Errorf("mihomo-pool: 创建 WorkDir 失败: %w", err)
	}
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("mihomo-pool: 打开日志失败: %w", err)
	}

	cmd := exec.Command(bin, "-d", b.opts.WorkDir, "-f", cfgPath)
	cmd.Dir = b.opts.WorkDir
	if len(b.opts.Env) > 0 {
		cmd.Env = append(os.Environ(), b.opts.Env...)
	}
	// 必须是 *os.File：交给 io.Writer 时 os/exec 会建管道并起 copier，
	// copier 遇到上游关闭就 broken pipe，内核也会跟着起不来。
	cmd.Stdout = logf
	cmd.Stderr = logf

	applyProcAttrs(cmd)

	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return fmt.Errorf("mihomo-pool: 启动内核失败: %w", err)
	}
	_ = logf.Close()

	b.mu.Lock()
	b.cmd = cmd
	b.bin = bin
	b.logPath = logPath
	b.lastExit = ""
	b.done = make(chan struct{})
	b.closing.Store(false)
	done := b.done
	b.mu.Unlock()

	go b.waitExit(cmd, done)
	return nil
}

// waitExit 等进程退出并回调。
func (b *processBackend) waitExit(cmd *exec.Cmd, done chan struct{}) {
	err := cmd.Wait()

	msg := ""
	if err != nil {
		msg = describeExit(err)
	}

	b.mu.Lock()
	b.cmd = nil
	b.lastExit = msg
	closing := b.closing.Load()
	fn := b.onExit
	b.mu.Unlock()

	close(done)

	// 主动停止不触发回调，避免上层误判为崩溃
	if !closing && fn != nil {
		fn(err)
	}
}

// Stop 停止内核进程。
func (b *processBackend) Stop(ctx context.Context) error {
	b.mu.Lock()
	cmd := b.cmd
	done := b.done
	if cmd == nil || cmd.Process == nil {
		b.mu.Unlock()
		return nil
	}
	b.closing.Store(true)
	b.mu.Unlock()

	if err := terminateProcess(cmd.Process); err != nil {
		_ = cmd.Process.Kill()
	}

	timeout := b.opts.StopTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}

	if done != nil {
		select {
		case <-done:
		case <-time.After(timeout):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		case <-ctx.Done():
			_ = cmd.Process.Kill()
		}
	} else {
		_ = cmd.Process.Kill()
	}
	return nil
}

// Reload 用信号让内核重载配置；Windows 走控制面（由上层 Kernel 处理）。
func (b *processBackend) Reload(ctx context.Context, cfg []byte, cfgPath string) error {
	b.mu.Lock()
	cmd := b.cmd
	b.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return errors.New("mihomo-pool: 内核未运行，无法重载")
	}
	return signalReload(cmd.Process)
}

// tailFile 读取文件尾部 n 行。
func tailFile(path string, n int) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()

	const maxRead = 64 << 10
	st, err := f.Stat()
	if err != nil {
		return nil
	}
	size := st.Size()
	var off int64
	if size > maxRead {
		off = size - maxRead
	}
	buf := make([]byte, size-off)
	if _, err := f.ReadAt(buf, off); err != nil && len(buf) == 0 {
		return nil
	}
	return splitTail(string(buf), n)
}

// trimSpace 去首尾空白。
func trimSpace(s string) string {
	return strings.TrimSpace(s)
}

// containsAny 判断字符串是否含任一字符。
func containsAny(s, chars string) bool {
	for _, c := range chars {
		if strings.ContainsRune(s, c) {
			return true
		}
	}
	return false
}

// splitTail 切出尾部 n 行并去空。
func splitTail(s string, n int) []string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// describeExit 把 exec 退出错误整理成一行。
func describeExit(err error) string {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if sig := signalName(ee); sig != "" {
			return "内核被信号终止: " + sig
		}
		return exitErrorString(ee.ExitCode(), "")
	}
	return "内核异常退出: " + err.Error()
}
