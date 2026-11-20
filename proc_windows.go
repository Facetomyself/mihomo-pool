//go:build windows

package mihomopool

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"unsafe"
)

// Windows 平台的进程控制实现。
//
// 关键点：
//   - Windows 没有进程组信号，优雅退出靠 CREATE_NEW_PROCESS_GROUP + CTRL_BREAK
//   - CTRL_BREAK 只能投递给带控制台的进程，守护进程场景下常失败，
//     所以失败即回落 Kill，这是 Windows 下的正常路径而不是错误
//   - 重载没有 SIGHUP，只能靠控制面 POST /configs 重新加载（configs 内容走 PUT）

var (
	kernel32                     = syscall.NewLazyDLL("kernel32.dll")
	procGenerateConsoleCtrlEvent = kernel32.NewProc("GenerateConsoleCtrlEvent")
)

const (
	createNewProcessGroup = 0x00000200
	ctrlBreakEvent        = 1
)

// applyProcAttrs 让子进程成为新进程组的组长，便于接收 CTRL_BREAK。
func applyProcAttrs(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= createNewProcessGroup
	cmd.SysProcAttr.HideWindow = true
}

// terminateProcess 尝试 CTRL_BREAK，失败回落 Kill。
func terminateProcess(p *os.Process) error {
	pid, err := strconv.ParseUint(fmt.Sprint(p.Pid), 10, 32)
	if err == nil {
		r, _, _ := procGenerateConsoleCtrlEvent.Call(
			uintptr(ctrlBreakEvent),
			uintptr(uint32(pid)),
		)
		if r != 0 {
			// 已下发，稍等由 Stop 的超时控制兜底
			return nil
		}
	}
	return p.Kill()
}

// signalReload 在 Windows 下没有等价信号，返回明确错误让调用方走控制面重载。
func signalReload(_ *os.Process) error {
	return fmt.Errorf("mihomo-pool: Windows 不支持信号重载，改用 SetConfig 走控制面")
}

// signalName 在 Windows 下没有 POSIX 信号语义。
func signalName(ee *exec.ExitError) string {
	if ws, ok := ee.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return "signal " + strconv.Itoa(int(ws.Signal()))
	}
	return ""
}

var _ = unsafe.Pointer(nil)
