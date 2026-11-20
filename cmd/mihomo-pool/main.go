// Command mihomo-pool 是给外部程序（尤其是 Python）调用的 CLI。
//
// 设计目标：一次启动、常驻进程、双向 JSONL 流式收发。
// Python 侧用 subprocess + asyncio 就能拿到真正的异步高并发，
// 不受 GIL 影响，也不需要每次调用重新起内核。
//
// 协议（每个方向都是「一行一个 JSON」）：
//
//	stdin  ← {"id":"1","op":"request","url":"https://...", ...}
//	stdout → {"id":"1","ok":true,"status":200,"body_b64":"...", ...}
//	stderr ← 只用于致命错误和 -v 调试日志，不参与协议
//
// 子命令：
//
//	serve      常驻 JSONL 服务（Python 主用）
//	call       单次调用，参数走命令行
//	batch      从 stdin/file 读多个请求，并发跑完输出 JSON 数组
//	start      只启动内核并常驻（把端口留着给别的进程用）
//	status     查内核状态
//	stop       停内核
//	health     并发测速
//	nodes      列出节点
//	version    打印版本
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mihomopool "github.com/Facetomyself/mihomo-pool"
)

// buildVersion 是 CLI 自身的版本，由 -ldflags 注入（见 .goreleaser.yaml）。
//
// 不注入时回落到库版本，避免出现 CLI 说 "dev"、库说 "1.0.0" 的漂移。
// 两者语义不同：lib_version 是 API 兼容性版本，cli_version 是发行版本
// （可以带 git 描述，例如 v1.0.0-3-gabc1234）。
var buildVersion = ""

// protocolVersion 是 JSONL 协议版本，Python 侧据此做兼容判断。
const protocolVersion = 1

// globalFlagNames 是可以在子命令之前出现的全局参数。
//
// 允许两种写法：
//
//	mihomo-pool serve -bin x -dir y
//	mihomo-pool -bin x -dir y serve
//
// 后者是 Python 客户端与多数命令行习惯，必须都支持。
var globalFlagNames = map[string]bool{
	"bin": true, "dir": true, "socks": true, "api": true, "base": true,
	"secret": true, "allow-lan": true, "log-level": true,
}

// splitGlobalArgs 把子命令之前的全局参数搬到子命令之后。
func splitGlobalArgs(argv []string) (cmd string, args []string) {
	i := 0
	var globals []string
	for i < len(argv) {
		a := argv[i]
		if !strings.HasPrefix(a, "-") {
			cmd = a
			i++
			break
		}
		name := strings.TrimLeft(a, "-")
		val := ""
		if idx := strings.IndexByte(name, '='); idx >= 0 {
			val = name[idx+1:]
			name = name[:idx]
		}
		if !globalFlagNames[name] {
			// 不认识的参数出现在子命令前：当成子命令处理，交给后续报错
			cmd = a
			i++
			break
		}
		globals = append(globals, a)
		if val == "" && name != "allow-lan" && i+1 < len(argv) {
			// 布尔参数不吃下一个 token
			globals = append(globals, argv[i+1])
			i += 2
			continue
		}
		i++
	}
	args = append(globals, argv[i:]...)
	return cmd, args
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := splitGlobalArgs(os.Args[1:])
	if cmd == "" {
		usage()
		os.Exit(2)
	}

	switch cmd {
	case "serve":
		os.Exit(runServe(args))
	case "call":
		os.Exit(runCall(args))
	case "batch":
		os.Exit(runBatch(args))
	case "start":
		os.Exit(runStart(args))
	case "status":
		os.Exit(runStatus(args))
	case "stop":
		os.Exit(runStop(args))
	case "health":
		os.Exit(runHealth(args))
	case "nodes":
		os.Exit(runNodes(args))
	case "version", "-v", "--version":
		emit(map[string]any{
			"ok":            true,
			"cli_version":   cliVersion(),
			"protocol":      protocolVersion,
			"lib_version":   mihomopool.Version,
			"default_ports": defaultPortSummary(),
		})
		os.Exit(0)
	case "help", "-h", "--help":
		usage()
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "未知子命令 %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `mihomo-pool - mihomo 多节点独立端口池 + 异步高并发请求

用法:
  mihomo-pool <子命令> [参数]

子命令:
  serve     常驻 JSONL 服务（Python 主用，一次启动多次调用）
  call      单次请求
  batch     批量并发请求（JSON 数组 stdin 或文件）
  start     启动内核并常驻，输出端口信息
  status    内核状态
  stop      停止内核
  health    并发测速
  nodes     列出节点
  version   版本信息

全局参数（除 version 外都接受）:
  -bin string      mihomo 可执行文件路径（默认自动查找）
  -dir string      工作目录（默认系统缓存目录/mihomo-pool）
  -socks int       共享端口，默认 17890
  -api int         控制面端口，默认 17891
  -base int        节点独立端口起点，默认 17900
  -secret string   控制面鉴权口令
  -allow-lan       允许局域网访问

Python 示例见 python/mihomo_pool/client.py
`)
}

func defaultPortSummary() map[string]int {
	o := mihomopool.DefaultOptions()
	return map[string]int{"socks": o.SocksPort, "api": o.APIPort, "base": o.BasePort}
}

// emit 输出一行 JSON 到 stdout。
func emit(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mihomo-pool: 序列化输出失败: %v\n", err)
		return
	}
	os.Stdout.Write(b)
	os.Stdout.Write([]byte("\n"))
}

// fail 输出一条错误并返回退出码。
func fail(msg string, extra map[string]any) int {
	out := map[string]any{"ok": false, "error": msg}
	for k, v := range extra {
		out[k] = v
	}
	emit(out)
	return 1
}

// commonFlags 是各子命令共享的 Options 参数。
type commonFlags struct {
	bin      string
	dir      string
	socks    int
	api      int
	base     int
	secret   string
	allowLAN bool
	logLevel string
}

// bind 把参数注册到 FlagSet。
func (c *commonFlags) bind(fs *flag.FlagSet) {
	d := mihomopool.DefaultOptions()
	fs.StringVar(&c.bin, "bin", "", "mihomo 可执行文件路径")
	fs.StringVar(&c.dir, "dir", "", "工作目录")
	fs.IntVar(&c.socks, "socks", d.SocksPort, "共享端口")
	fs.IntVar(&c.api, "api", d.APIPort, "控制面端口")
	fs.IntVar(&c.base, "base", d.BasePort, "节点独立端口起点")
	fs.StringVar(&c.secret, "secret", "", "控制面鉴权口令")
	fs.BoolVar(&c.allowLAN, "allow-lan", false, "允许局域网访问")
	fs.StringVar(&c.logLevel, "log-level", "", "内核日志级别")
}

// toOptions 转成库选项。
func (c *commonFlags) toOptions() mihomopool.Options {
	o := mihomopool.DefaultOptions()
	o.Binary = c.bin
	o.WorkDir = c.dir
	o.SocksPort = c.socks
	o.APIPort = c.api
	o.BasePort = c.base
	o.Secret = c.secret
	o.AllowLAN = c.allowLAN
	if c.logLevel != "" {
		o.LogLevel = c.logLevel
	}
	return o
}

// signalContext 返回一个在收到 INT/TERM 时取消的 context。
func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

// sleepCtx 可被取消的等待。
func sleepCtx(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// trimNL 去掉行尾换行。
func trimNL(s string) string {
	return strings.TrimRight(s, "\r\n")
}

// newReader 返回带缓冲的 stdin 行读取器。
func newReader() *bufio.Reader {
	return bufio.NewReaderSize(os.Stdin, 1<<20)
}

// cliVersion 返回 CLI 版本：优先用注入值，否则回落到库版本。
func cliVersion() string {
	if strings.TrimSpace(buildVersion) != "" {
		return buildVersion
	}
	return mihomopool.Version
}
