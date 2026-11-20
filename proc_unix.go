//go:build !windows

package mihomopool

import (
	"os"
	"os/exec"
	"syscall"
)

// 非 Windows 平台的进程控制实现。
//
// 关键点：内核会 fork 出子进程（DNS、provider 下载），必须放进独立进程组，
// 停止时对整组发信号，否则子进程会变成孤儿继续占端口。

// applyProcAttrs 让子进程进入独立进程组，父进程的信号不会被它继承。
func applyProcAttrs(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// terminateProcess 先发 SIGTERM 让内核优雅关闭，失败再强杀。
func terminateProcess(p *os.Process) error {
	pgid, err := syscall.Getpgid(p.Pid)
	if err == nil && pgid > 0 {
		// 负 pid = 对整个进程组发信号
		if err := syscall.Kill(-pgid, syscall.SIGTERM); err == nil {
			return nil
		}
	}
	return p.Signal(syscall.SIGTERM)
}

// signalReload 用 SIGHUP 触发 mihomo 重载配置。
func signalReload(p *os.Process) error {
	pgid, err := syscall.Getpgid(p.Pid)
	if err == nil && pgid > 0 {
		if err := syscall.Kill(-pgid, syscall.SIGHUP); err == nil {
			return nil
		}
	}
	return p.Signal(syscall.SIGHUP)
}

// signalName 把退出信息还原成信号名。
func signalName(ee *exec.ExitError) string {
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return ws.Signal().String()
	}
	return ""
}
