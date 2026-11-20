package mihomopool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kernel 管理内核运行时。
//
// 具体实现由 backend 决定：
//   - 默认：mihomo 作为独立子进程（进程隔离、可崩溃重启）
//   - -tags embedded：mihomo 链接进本进程（启动快、单文件分发）
//
// 两者对上层完全透明：Pool / Executor / CLI 都只看到 Kernel。
type Kernel struct {
	opts    Options
	backend backend

	mu        sync.Mutex
	state     ProcessState
	startedAt time.Time
	restarts  int
	logs      *ringBuffer

	readyCh chan struct{}
	closing bool

	configPath string
	bindings   []listenerBinding
	nodesSpec  []NodeSpec
	providers  []ProviderSpec
}

// newKernel 构造（不启动）。
func newKernel(o Options) *Kernel {
	k := &Kernel{
		opts:  o,
		state: StateStopped,
		logs:  newRingBuffer(200),
	}
	b, err := newBackend(o)
	if err != nil {
		return k
	}
	k.backend = b
	k.backend.SetOnExit(k.onBackendExit)
	return k
}

// BackendName 返回当前构建使用的内核后端名（process / embedded）。
func (k *Kernel) BackendName() string {
	if k.backend == nil {
		return "none"
	}
	return k.backend.Name()
}

// onBackendExit 处理子进程异常退出，按退避重启。
func (k *Kernel) onBackendExit(err error) {
	k.mu.Lock()
	if k.closing || k.state == StateStopping {
		k.mu.Unlock()
		return
	}
	restarts := k.restarts
	if restarts >= k.opts.MaxRestarts {
		k.state = StateCrashed
		k.mu.Unlock()
		return
	}
	k.restarts++
	k.state = StateCrashed
	nodes := k.nodesSpec
	providers := k.providers
	backoffN := k.restarts
	k.mu.Unlock()

	backoff := time.Duration(backoffN) * k.opts.RestartBackoff
	go func() {
		select {
		case <-time.After(backoff):
		case <-time.After(time.Duration(k.opts.MaxRestarts)*k.opts.RestartBackoff + time.Second):
		}

		text, bindings, rerr := k.opts.renderConfig(nodes, providers)
		if rerr != nil {
			return
		}
		path, werr := k.opts.writeConfig(text)
		if werr != nil {
			return
		}
		if serr := k.backend.Start(context.Background(), []byte(text), path); serr != nil {
			return
		}
		k.mu.Lock()
		k.configPath = path
		k.bindings = bindings
		k.state = StateRunning
		k.mu.Unlock()
	}()
}

// Start 生成配置并启动内核，阻塞到端口就绪。
func (k *Kernel) Start(ctx context.Context, nodes []NodeSpec, providers []ProviderSpec) error {
	if k.backend == nil {
		return errors.New("mihomo-pool: 没有可用的内核后端")
	}

	k.mu.Lock()
	if k.state == StateRunning || k.state == StateStarting {
		k.mu.Unlock()
		return errors.New("mihomo-pool: 内核已在运行")
	}
	k.closing = false
	k.state = StateStarting
	k.mu.Unlock()

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

	if err := k.backend.Start(ctx, []byte(text), cfgPath); err != nil {
		k.setState(StateStopped)
		return err
	}

	k.mu.Lock()
	k.configPath = cfgPath
	k.bindings = bindings
	k.nodesSpec = nodes
	k.providers = providers
	k.startedAt = time.Now()
	k.mu.Unlock()

	if err := k.waitReady(ctx); err != nil {
		_ = k.Stop(context.Background())
		return err
	}

	k.mu.Lock()
	k.state = StateRunning
	k.restarts = 0
	k.mu.Unlock()
	return nil
}

// waitReady 轮询到端口可用。
func (k *Kernel) waitReady(parent context.Context) error {
	timeout := k.opts.StartTimeout
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	tick := time.NewTicker(60 * time.Millisecond)
	defer tick.Stop()

	needSocks := len(k.Bindings()) > 0 || !k.opts.SkipSharedProbe

	for {
		select {
		case <-ctx.Done():
			tail := k.backend.LogTail(15)
			msg := fmt.Sprintf("mihomo-pool: 等待内核就绪超时（%s）", timeout)
			if le := k.backend.LastExit(); le != "" {
				msg += "；" + le
			}
			if len(tail) > 0 {
				msg += "；日志尾部: " + strings.Join(tail, " | ")
			}
			return errors.New(msg)
		case <-tick.C:
			if !k.backend.Running() {
				return fmt.Errorf("mihomo-pool: 内核启动后立即退出: %s", k.exitMessage())
			}
			if portOpen(k.opts.APIPort) && (!needSocks || portOpen(k.opts.SocksPort)) {
				return nil
			}
		}
	}
}

// exitMessage 返回最近一次退出描述。
func (k *Kernel) exitMessage() string {
	if k.backend == nil {
		return "未知原因"
	}
	if m := k.backend.LastExit(); m != "" {
		return m
	}
	return "未知原因"
}

// Stop 停止内核。
func (k *Kernel) Stop(ctx context.Context) error {
	k.mu.Lock()
	k.closing = true
	k.state = StateStopping
	k.mu.Unlock()

	if k.backend != nil {
		_ = k.backend.Stop(ctx)
	}

	k.setState(StateStopped)
	return nil
}

// Reload 用新配置重载。
func (k *Kernel) Reload(nodes []NodeSpec, providers []ProviderSpec) error {
	if k.backend == nil || !k.backend.Running() {
		return errors.New("mihomo-pool: 内核未运行，无法重载")
	}

	text, bindings, err := k.opts.renderConfig(nodes, providers)
	if err != nil {
		return err
	}
	path, err := k.opts.writeConfig(text)
	if err != nil {
		return err
	}

	k.mu.Lock()
	k.configPath = path
	k.bindings = bindings
	k.nodesSpec = nodes
	k.providers = providers
	k.mu.Unlock()

	return k.backend.Reload(context.Background(), []byte(text), path)
}

// Status 返回状态快照。
func (k *Kernel) Status() KernelStatus {
	k.mu.Lock()
	st := KernelStatus{
		State:      k.state,
		StartedAt:  k.startedAt,
		Restarts:   k.restarts,
		ConfigPath: k.configPath,
	}
	k.mu.Unlock()

	if k.backend != nil {
		st.PID = k.backend.PID()
		st.LastExit = k.backend.LastExit()
		st.LogTail = k.backend.LogTail(40)
		st.Backend = k.backend.Name()
	}
	if st.LogTail == nil {
		st.LogTail = k.logs.lines(40)
	}
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
	out = append(out, r.lines_[start:]...)
	if s := strings.TrimSpace(string(r.cur)); s != "" && len(out) < n {
		out = append(out, s)
	}
	return out
}
