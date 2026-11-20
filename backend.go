package mihomopool

import (
	"context"
	"errors"
)

// backend 是内核运行时的抽象。
//
// 两种实现：
//   - processBackend：把 mihomo 当独立子进程跑（默认，见 kernel.go）
//   - embedBackend：  把 mihomo 链接进本进程（-tags embedded，见 embed_*.go）
//
// Kernel 只依赖这个接口，因此上层的 Pool / Executor / CLI 完全不用关心
// 内核是以什么形态运行的。
type backend interface {
	// Name 返回后端名，用于状态与日志。
	Name() string

	// Start 用给定配置启动内核。cfgPath 是磁盘上的配置文件路径
	// （process 后端读它，embed 后端直接用 renderConfig 的产物）。
	Start(ctx context.Context, cfg []byte, cfgPath string) error

	// Stop 停止内核。必须幂等。
	Stop(ctx context.Context) error

	// Reload 用新配置重载，不重启进程（embed 后端与 process 后端实现不同）。
	Reload(ctx context.Context, cfg []byte, cfgPath string) error

	// PID 返回内核 pid；embed 后端返回本进程 pid。
	PID() int

	// Running 判断内核当前是否在运行。
	Running() bool

	// LastExit 返回最近一次异常退出的描述（正常运行时为空）。
	LastExit() string

	// LogTail 返回最近 n 行内核日志。
	LogTail(n int) []string

	// SetOnExit 注册退出回调，用于进程后端的崩溃重启。
	// embed 后端不会退出，因此可以忽略。
	SetOnExit(fn func(err error))
}

// errBackendUnavailable 表示当前构建不包含该后端。
var errBackendUnavailable = errors.New("mihomo-pool: 当前构建不包含该内核后端")
