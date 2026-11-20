package mihomopool

import (
	"net"
	"testing"
)

// freePort 找一个当前空闲的本地端口。
//
// 注意：拿到端口后到真正监听之间有空窗，并发场景下仍可能被抢。
// 测试里够用；生产代码不做这种探测（见 Options.BasePort 的说明）。
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("找空闲端口失败: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
