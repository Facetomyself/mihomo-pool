package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 内核进程生命周期。
//
// 跨平台要点：
//   - 进程组隔离由 proc_unix.go / proc_windows.go 各自实现，这里只调抽象
//   - 优雅退出先发 TERM/CTRL_BREAK，超时再强杀
//   - 内核异常退出后按 RestartBackoff 退避重启，次数受 MaxRestarts 限制
//   - 崩溃时保留日志尾部，便于定位配置错误

// Kernel 管理一个 mihomo 子进程。
type Kernel struct {
	opts Options

	mu        sync.Mutex
	cmd       *exec.Cmd
	state     ProcessState
	pid       int
	startedAt time.Time
	lastExit  string
	restarts  int
	logTail   *ringBuffer

	readyCh  chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
	closing  bool

	configPath string
	logPath    string
	bindings   []listenerBinding
}

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
}

// newKernel 构造（不启动）。
func newKernel(o Options) *Kernel {
	return &Kernel{
		opts:    o,
		state:   StateStopped,
		logTail: newRingBuffer(200),
	}
}

// resolveBinary 按搜索链找可执行文件。
func (k *Kernel) resolveBinary() (string, error) {
	if b := strings.TrimSpace(k.opts.Binary); b != "" {
		if st, err := os.Stat(b); err == nil && !st.IsDir() {
			return b, nil
		}
		return "", fmt.Errorf("mihomo-pool: Binary 指定为 %q 但文件不存在", b)
	}
	cands := k.opts.SearchPaths
	if len(cands) == 0 {
		cands = defaultSearchPaths(&k.opts)
	}
	for _, c := range cands {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if filepath.IsAbs(c) || strings.ContainsAny(c, `/\`) {
			if st, err := os.Stat(c); err == nil && !st.IsDir() {
				return c, nil
			}
			continue
		}
		if p, err := exec.LookPath(c); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("mihomo-pool: 未找到 mihomo 内核，已尝试 %d 个位置（把可执行文件放到 %s 或设置 Options.Binary）",
		len(cands), filepath.Join(k.opts.WorkDir, "bin"))
}

// Start 生成配置并拉起内核，阻塞到端口就绪。
func (k *Kernel) Start(ctx context.Context, nodes []NodeSpec, providers []ProviderSpec) error {
	k.mu.Lock()
	if k.state == StateRunning || k.state == StateStarting {
		k.mu.Unlock()
		return errors.New("mihomo-pool: 内核已在运行")
	}
	k.closing = false
	k.state = StateStarting
	k.mu.Unlock()

	bin, err := k.resolveBinary()
	if err != nil {
		k.setState(StateStopped)
		return err
	}

	text, bindings, err := k.opts.renderConfig(nodes, providers)
	if err != nil {
		k.setState(StateStopped)
		return err
	}
	cfgPath, err := k.opts.writeConfig(text)
	if err != nil {
		k.setState(StateStopped)
		return err
	}

	k.mu.Lock()
	k.configPath = cfgPath
	k.bindings = bindings
	k.mu.Unlock()

	if err := k.spawn(bin, cfgPath); err != nil {
		k.setState(StateStopped)
		return err
	}

	ready := make(chan struct{})
	k.mu.Lock()
	k.readyCh = ready
	k.mu.Unlock()

	go k.waitExit()

	if err := k.waitReady(ctx); err != nil {
		_ = k.Stop(context.Background())
		return err
	}
	return nil
}

// spawn 启动进程并接管输出。
func (k *Kernel) spawn(bin, cfgPath string) error {
	logPath := filepath.Join(k.opts.WorkDir, "mihomo.log")
	if err := os.MkdirAll(k.opts.WorkDir, 0o755); err != nil {
		return fmt.Errorf("mihomo-pool: 创建 WorkDir 失败: %w", err)
	}
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("mihomo-pool: 打开日志失败: %w", err)
	}

	cmd := exec.Command(bin, "-d", k.opts.WorkDir, "-f", cfgPath)
	cmd.Dir = k.opts.WorkDir
	if len(k.opts.Env) > 0 {
		cmd.Env = append(os.Environ(), k.opts.Env...)
	}
	// 必须是 *os.File 本身：交给 io.Writer 时 os/exec 会建管道并起 copier，
	// 而 copier 一旦遇到上游关闭就报 broken pipe，内核也会跟着起不来。
	// 日志尾部不从这里旁路采集，改由 logTailFromFile 读文件。
	cmd.Stdout = logf
	cmd.Stderr = logf

	applyProcAttrs(cmd)

	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return fmt.Errorf("mihomo-pool: 启动内核失败: %w", err)
	}
	k.mu.Lock()
	k.logPath = logPath
	k.mu.Unlock()
	// 子进程持有自己的句柄副本，父进程这份可以关
	_ = logf.Close()

	k.mu.Lock()
	k.cmd = cmd
	k.pid = cmd.Process.Pid
	k.state = StateStarting
	k.startedAt = time.Now()
	k.mu.Unlock()
	return nil
}

// waitExit 等待进程退出并处理重启。
func (k *Kernel) waitExit() {
	k.mu.Lock()
	cmd := k.cmd
	k.mu.Unlock()
	if cmd == nil {
		return
	}
	err := cmd.Wait()

	k.mu.Lock()
	exitMsg := "内核正常退出"
	if err != nil {
		exitMsg = describeExit(err)
	}
	k.lastExit = exitMsg
	k.pid = 0
	k.cmd = nil
	closing := k.closing
	state := k.state
	ready := k.readyCh
	k.state = StateStopped
	if ready != nil {
		select {
		case <-ready:
		default:
			close(ready)
		}
	}
	done := k.doneCh
	k.doneCh = nil
	k.mu.Unlock()

	if done != nil {
		close(done)
	}

	// 主动停止或启动阶段失败：不重启
	if closing || state == StateStopping {
		return
	}

	k.mu.Lock()
	restarts := k.restarts
	k.mu.Unlock()
	if restarts >= k.opts.MaxRestarts {
		k.setState(StateCrashed)
		return
	}

	backoff := time.Duration(restarts+1) * k.opts.RestartBackoff
	k.mu.Lock()
	k.restarts++
	k.state = StateCrashed
	k.mu.Unlock()

	go func() {
		select {
		case <-time.After(backoff):
		case <-k.stopping():
			return
		}
		bin, err := k.resolveBinary()
		if err != nil {
			return
		}
		if err := k.spawn(bin, k.configPath); err != nil {
			return
		}
		ready := make(chan struct{})
		k.mu.Lock()
		k.readyCh = ready
		k.mu.Unlock()
		go k.waitExit()
		ctx, cancel := context.WithTimeout(context.Background(), k.opts.StartTimeout)
		defer cancel()
		_ = k.waitReady(ctx)
	}()
}

// stopping 返回一个在 Stop 被调用时关闭的通道。
func (k *Kernel) stopping() <-chan struct{} {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.readyCh == nil {
		k.readyCh = make(chan struct{})
	}
	return k.readyCh
}

// waitReady 轮询到 SOCKS 端口与控制面端口都可用。
func (k *Kernel) waitReady(parent context.Context) error {
	timeout := k.opts.StartTimeout
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	tick := time.NewTicker(120 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			k.mu.Lock()
			exit := k.lastExit
			tail := k.tailLog(15)
			k.mu.Unlock()
			msg := fmt.Sprintf("mihomo-pool: 等待内核就绪超时（%s）", timeout)
			if exit != "" {
				msg += "；" + exit
			}
			if len(tail) > 0 {
				msg += "；日志尾部: " + strings.Join(tail, " | ")
			}
			return errors.New(msg)
		case <-tick.C:
			k.mu.Lock()
			state := k.state
			pid := k.pid
			k.mu.Unlock()
			if pid == 0 && state != StateStarting {
				return fmt.Errorf("mihomo-pool: 内核启动后立即退出: %s", k.exitMessage())
			}
			if portOpen(k.opts.APIPort) && (portOpen(k.opts.SocksPort) || len(k.Bindings()) == 0) {
				k.mu.Lock()
				k.state = StateRunning
				k.restarts = 0
				ready := k.readyCh
				k.readyCh = nil
				k.mu.Unlock()
				if ready != nil {
					// 用 doneCh 作为停止信号的替身：running 后 readyCh 置空
					select {
					case <-ready:
					default:
						close(ready)
					}
				}
				return nil
			}
		}
	}
}

// exitMessage 返回最近一次退出描述。
func (k *Kernel) exitMessage() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.lastExit == "" {
		return "未知原因"
	}
	return k.lastExit
}

// Stop 优雅停止内核，超时强杀。
func (k *Kernel) Stop(ctx context.Context) error {
	k.mu.Lock()
	if k.state == StateStopped && k.cmd == nil {
		k.mu.Unlock()
		return nil
	}
	k.closing = true
	k.state = StateStopping
	cmd := k.cmd
	done := k.doneCh
	k.mu.Unlock()

	if cmd == nil || cmd.Process == nil {
		k.setState(StateStopped)
		return nil
	}

	if done == nil {
		done = make(chan struct{})
		k.mu.Lock()
		k.doneCh = done
		k.mu.Unlock()
	}

	if err := terminateProcess(cmd.Process); err != nil {
		// TERM 发不出去就直接强杀
		_ = cmd.Process.Kill()
	}

	timeout := k.opts.StopTimeout
	if deadline, ok := ctx.Deadline(); ok {
		if d := time.Until(deadline); d > 0 && d < timeout {
			timeout = d
		}
	}

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

	k.setState(StateStopped)
	return nil
}

// Reload 用新配置重载运行中的内核（不重启进程）。
//
// 靠 SIGHUP（Windows 下走外部控制面 PUT /configs?force=false 的等价路径）。
func (k *Kernel) Reload(nodes []NodeSpec, providers []ProviderSpec) error {
	k.mu.Lock()
	cmd := k.cmd
	cfgPath := k.configPath
	k.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return errors.New("mihomo-pool: 内核未运行，无法重载")
	}

	text, bindings, err := k.opts.renderConfig(nodes, providers)
	if err != nil {
		return err
	}
	newPath, err := k.opts.writeConfig(text)
	if err != nil {
		return err
	}
	if newPath != cfgPath {
		k.mu.Lock()
		k.configPath = newPath
		k.mu.Unlock()
	}
	k.mu.Lock()
	k.bindings = bindings
	k.mu.Unlock()

	return signalReload(cmd.Process)
}

// Status 返回状态快照。
func (k *Kernel) Status() KernelStatus {
	k.mu.Lock()
	st := KernelStatus{
		State:      k.state,
		PID:        k.pid,
		StartedAt:  k.startedAt,
		LastExit:   k.lastExit,
		Restarts:   k.restarts,
		ConfigPath: k.configPath,
	}
	logPath := k.logPath
	k.mu.Unlock()

	// tailLog 自己会加锁，必须在解锁之后调用：
	// sync.Mutex 不可重入，在持锁期间再进 tailLog 会直接死锁。
	st.LogTail = k.tailLogFrom(logPath, 40)
	return st
}

// Bindings 返回节点到本地端口的映射副本。
func (k *Kernel) Bindings() []listenerBinding {
	k.mu.Lock()
	defer k.mu.Unlock()
	out := make([]listenerBinding, len(k.bindings))
	copy(out, k.bindings)
	return out
}

// setState 设置状态。
func (k *Kernel) setState(s ProcessState) {
	k.mu.Lock()
	k.state = s
	k.mu.Unlock()
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

// portOpen 探测本地 TCP 端口是否可连。
func portOpen(port int) bool {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:"+strconv.Itoa(port), 300*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// ringBuffer 是并发安全的日志尾部缓冲。
type ringBuffer struct {
	mu     sync.Mutex
	max    int
	lines_ []string
	cur    []byte
}

func newRingBuffer(max int) *ringBuffer {
	return &ringBuffer{max: max, lines_: make([]string, 0, max)}
}

// Write 实现 io.Writer。
func (r *ringBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, b := range p {
		if b == '\n' {
			line := strings.TrimSpace(string(r.cur))
			r.cur = r.cur[:0]
			if line != "" {
				r.push(line)
			}
			continue
		}
		r.cur = append(r.cur, b)
		if len(r.cur) > 8192 {
			line := strings.TrimSpace(string(r.cur))
			r.cur = r.cur[:0]
			if line != "" {
				r.push(line)
			}
		}
	}
	return len(p), nil
}

func (r *ringBuffer) push(line string) {
	if len(r.lines_) >= r.max {
		copy(r.lines_, r.lines_[1:])
		r.lines_[len(r.lines_)-1] = line
		return
	}
	r.lines_ = append(r.lines_, line)
}

func (r *ringBuffer) lines(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, n)
	start := len(r.lines_) - n
	if start < 0 {
		start = 0
	}
	for _, l := range r.lines_[start:] {
		out = append(out, l)
	}
	if s := strings.TrimSpace(string(r.cur)); s != "" && len(out) < n {
		out = append(out, s)
	}
	return out
}

// tailLog 从日志文件尾部读取最近 n 行。
//
// 不再用内存环形缓冲旁路：stdout 必须直接交给 *os.File，
// 否则 os/exec 会插一层管道 copier，异常时反而把内核拖死。
func (k *Kernel) tailLog(n int) []string {
	k.mu.Lock()
	path := k.logPath
	k.mu.Unlock()
	return k.tailLogFrom(path, n)
}

// tailLogFrom 读取指定日志文件的尾部 n 行，不碰 k.mu。
func (k *Kernel) tailLogFrom(path string, n int) []string {
	if path == "" {
		return k.logTail.lines(n)
	}
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
	off := int64(0)
	if size > maxRead {
		off = size - maxRead
	}
	buf := make([]byte, size-off)
	if _, err := f.ReadAt(buf, off); err != nil && len(buf) == 0 {
		return nil
	}
	lines := strings.Split(strings.TrimRight(string(buf), "\n"), "\n")
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
