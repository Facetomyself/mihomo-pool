# mihomo-pool

[![ci](https://github.com/Facetomyself/mihomo-pool/actions/workflows/ci.yml/badge.svg)](https://github.com/Facetomyself/mihomo-pool/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Facetomyself/mihomo-pool.svg)](https://pkg.go.dev/github.com/Facetomyself/mihomo-pool)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

把 [mihomo](https://github.com/MetaCubeX/mihomo) 内核的多节点出站变成一组互不干扰的本地代理端口，供 Go / Python 程序做异步高并发请求分发。独立库，零上层依赖，Windows / macOS / Linux 通用。

> **依赖关系**：本项目**调用** [mihomo](https://github.com/MetaCubeX/mihomo)（Clash Meta）作为代理内核，
> 以独立进程方式运行，不链接其代码。内核需要单独安装，见下方"安装"。
> 关于把它编译进来的利弊，见 [内嵌内核说明](#关于内嵌内核的说明)。

> **AI 助手用：** 本仓库自带一份技能说明 [skills/mihomo-pool/SKILL.md](skills/mihomo-pool/SKILL.md)，
> 用一句话讲清"这个工具解决什么问题、怎么用、真实机场有哪些坑"。把它加进你的 skills 目录即可被自动加载。

## 为什么不是"单端口 + 切 selector"

mihomo 的 `mixed-port` 只有一套规则和一个 `GLOBAL` selector。用 `PUT /proxies/GLOBAL` 切节点时，切换是**全局状态**：请求 A 正在用香港节点，请求 B 一进来切到日本，A 的连接就被连带影响。高并发下这种争用不可接受。

本库的做法是：**一个内核进程，N 条 listener**。每个节点在配置里独立占一条 `listeners` 条目，用 `listeners[n].proxy` 绑定到该出站：

```yaml
listeners:
  - name: out-hk-1
    type: mixed
    port: 17900
    listen: 127.0.0.1
    proxy: hk-1
  - name: out-us-1
    type: mixed
    port: 17901
    listen: 127.0.0.1
    proxy: us-1
```

于是 N 个节点就是 N 个本地端口，`socks5://127.0.0.1:17900` 永远走 `hk-1`，互不牵扯。分发器只需把请求发到对应端口，不需要和内核的控制面抢状态。

## 关于"内嵌内核"的说明

mihomo 本身是 Go 项目，理论上可以 `import` 进来当库用，不用外部二进制。
**实测结论：能做，但这个项目默认不做**，理由如下（都是实测数据，不是估计）：

| 维度 | 外部二进制（当前方案） | 内嵌进 mihomo 库 |
|---|---|---|
| 发布包体积 | 5 MB | **45–48 MB** |
| 依赖数量 | 1 个（yaml.v3） | **170 个模块 / 736 条依赖边** |
| 构建时间 | 秒级 | 数分钟 |
| 内核启动 | 100ms~2s（子进程） | **17ms**（同进程） |
| 崩溃影响 | 只死内核，主进程可重启 | **主进程一起挂** |
| 配置调整 | 改文本后重载 | 直接改 `RawConfig` 对象 |
| geodata | 内核工作目录自带 | **需自己准备 MMDB/GeoSite** |
| 版本升级 | 换个二进制 | 重新编译 + 全量回归 |

**内嵌确实可行的部分**（已实测跑通）：

```go
import (
    "github.com/metacubex/mihomo/config"
    "github.com/metacubex/mihomo/constant"
    "github.com/metacubex/mihomo/hub/executor"
)

// 1. 内嵌后没有 -d 工作目录，必须自己指定 geodata 位置
constant.SetHomeDir("/path/to/geodata")   // 放 geoip.metadb、GeoSite.dat

// 2. 解析成对象再改字段（改文本拼接会撞重复键）
raw, _ := config.UnmarshalRawConfig(subYAML)
raw.MixedPort = 17890
raw.ExternalController = "127.0.0.1:17891"
raw.Listeners = append(raw.Listeners, map[string]any{
    "name": "out-hk", "type": "mixed", "port": 17900,
    "listen": "127.0.0.1", "proxy": "hk-1",
})

// 3. 构建并应用
cfg, _ := config.ParseRawConfig(raw)
executor.ApplyConfig(cfg, true)

// 4. 关闭（异步，需要等）
executor.Shutdown()
```

实测数据（真实机场订阅，46 节点）：内嵌启动 17ms、解析 3.3ms、应用配置 364ms、
20 并发真实请求 20/20 成功、出口 IP 正确。

**为什么仍然默认用外部二进制：**

1. **体积涨 9 倍**。5 MB 的包对用户是零成本；45 MB 的包会让很多人犹豫。
   而现在的方案用户本来就要装 mihomo 内核（55 MB），总占用其实差不多——
   但"按需组合"比"强制捆绑"更灵活。
2. **170 个依赖**意味着任何一条依赖爆 CVE 都要重新发版。外部二进制方案里，
   mihomo 的安全更新由上游负责，我们只需换个二进制。
3. **崩溃隔离**。mihomo 的 bug、OOM、DNS 死锁在内嵌方案里会直接带走主进程。
   外部进程方案下内核挂了可以自动重启，主进程和请求队列都活着。
4. **geodata 管理**。外部二进制用 `-d` 目录自带 MMDB；内嵌必须自己准备并
   负责更新（GEOIP 规则缺 MMDB 会直接让 `ParseRawConfig` 失败，这个坑我踩过）。

**什么情况下内嵌更好：**

- 你要的是一个**单文件可分发的产品**（用户不想装第二个程序）
- 你的并发/延迟极度敏感，100ms 的子进程启动开销不可接受
- 你需要**程序化操纵配置对象**（例如按请求动态改 DNS、改规则），改文本做不到

需要的话，可以做成 build tag 切换：

```go
//go:build embedded

package mihomopool
// 用 import mihomo 的实现
```
配合 `-tags embedded` 编译。这样默认轻量、需要时内嵌，不用二选一。

## 安装

### 预编译二进制（推荐）

从 [Releases](https://github.com/Facetomyself/mihomo-pool/releases) 下载对应平台：

| 平台 | 文件名 |
|---|---|
| Linux x86_64 | `mihomo-pool_Linux_x86_64.tar.gz` |
| Linux arm64 | `mihomo-pool_Linux_arm64.tar.gz` |
| macOS Intel | `mihomo-pool_Darwin_x86_64.tar.gz` |
| macOS Apple Silicon | `mihomo-pool_Darwin_arm64.tar.gz` |
| Windows x64 | `mihomo-pool_Windows_x86_64.zip` |
| Windows arm64 | `mihomo-pool_Windows_arm64.zip` |

```bash
# Linux / macOS：自动选对架构
OS=$(uname -s); ARCH=$(uname -m)
[ "$ARCH" = "aarch64" ] && ARCH=arm64
[ "$ARCH" = "x86_64" ]  && ARCH=x86_64
case "$OS" in Darwin) ARCH=$([ "$ARCH" = "arm64" ] && echo arm64 || echo x86_64);; esac

TAG=v1.0.0
curl -sL -o mp.tar.gz "https://github.com/Facetomyself/mihomo-pool/releases/download/${TAG}/mihomo-pool_${OS}_${ARCH}.tar.gz"
curl -sL -o sums.txt "https://github.com/Facetomyself/mihomo-pool/releases/download/${TAG}/checksums.txt"
grep "mihomo-pool_${OS}_${ARCH}.tar.gz" sums.txt > one.sha && sha256sum -c one.sha
tar xzf mp.tar.gz && sudo mv mihomo-pool /usr/local/bin/
```

包内已含 `python/`（纯标准库客户端）、`skills/mihomo-pool/SKILL.md`、`CHANGELOG.md`。

### 从源码

```bash
git clone https://github.com/Facetomyself/mihomo-pool
cd mihomo-pool
go build -o /usr/local/bin/mihomo-pool ./cmd/mihomo-pool
```

### 还需要 mihomo 内核

内核来自 [MetaCubeX/mihomo](https://github.com/MetaCubeX/mihomo/releases)（Clash Meta，
支持 ss / vmess / vless / trojan / hysteria2 / tuic / wireguard 等协议）。

```bash
# Linux x86_64
VER=$(curl -s https://api.github.com/repos/MetaCubeX/mihomo/releases/latest | grep -oP '"tag_name": "\K[^"]+')
curl -sL -o /tmp/mihomo.gz "https://github.com/MetaCubeX/mihomo/releases/download/${VER}/mihomo-linux-amd64-compatible-${VER}.gz"
gunzip -f /tmp/mihomo.gz && chmod +x /tmp/mihomo && sudo mv /tmp/mihomo /usr/local/bin/
mihomo -v
```

macOS / Windows 从 [mihomo releases](https://github.com/MetaCubeX/mihomo/releases) 下对应包。

## 版本

```bash
mihomo-pool version
# {"cli_version":"1.0.0","lib_version":"1.0.0","protocol":1,"default_ports":{...}}
```

- `cli_version` — 发行版本（git tag），随每次发布变
- `lib_version` — Go 库 API 兼容版本，只在不兼容改动时跳
- `protocol` — CLI 与 Python 客户端的 JSONL 协议版本，不匹配会直接报错

变更记录见 [CHANGELOG.md](CHANGELOG.md)，发版流程见 [RELEASING.md](RELEASING.md)。

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "log"

    mihomopool "github.com/Facetomyself/mihomo-pool"
)

func main() {
    o := mihomopool.DefaultOptions()
    o.Binary = "/usr/local/bin/mihomo"     // 为空则按搜索链查找
    o.WorkDir = "./data/mihomo-pool"
    o.SocksPort = 17890                    // 共享端口（规则分流）
    o.APIPort = 17891                      // external-controller
    o.BasePort = 17900                     // 每节点独立端口起点

    pool, err := mihomopool.New(o)
    if err != nil {
        log.Fatal(err)
    }

    nodes := []mihomopool.NodeSpec{
        {Name: "hk-1", Type: "ss", Server: "1.2.3.4", Port: 8388,
         Raw: map[string]any{"cipher": "aes-256-gcm", "password": "xxx"}, Group: "hk"},
        {Name: "us-1", Type: "trojan", Server: "5.6.7.8", Port: 443,
         Raw: map[string]any{"password": "yyy", "sni": "a.example.com"}, Group: "us"},
    }

    ctx := context.Background()
    if err := pool.Start(ctx, nodes, nil); err != nil {
        log.Fatal(err)
    }
    defer pool.Stop(ctx)

    // 20 并发，自动挑节点，失败换节点重试
    pool.ConfigureExecutor(20, 8) // 20 workers，单节点最多 8 在飞

    reqs := make([]*mihomopool.Request, 0, 100)
    for i := 0; i < 100; i++ {
        reqs = append(reqs, &mihomopool.Request{
            URL:     fmt.Sprintf("https://target.example.com/item/%d", i),
            Timeout: 10 * 1e9,
            Retries: 2,
        })
    }
    ch, _ := pool.Executor().RunAsync(ctx, reqs)
    for resp := range ch {
        fmt.Printf("%s  status=%d  body=%d bytes\n", resp.Node, resp.Status, len(resp.Body))
    }
}
```

## 订阅

两条路线，按需求选：

### 交给内核对拉（推荐）

```go
providers := []mihomopool.ProviderSpec{
    {Name: "airport", URL: "https://sub.example.com/api/v1/client/subscribe?token=xxx", IntervalSec: 3600},
}
pool.Start(ctx, nil, providers)
// 起完从控制面同步节点
pool.RefreshNodes(ctx)
```

节点由 mihomo 自己定时更新，订阅 URL 只出现在内核配置里。缺点是这些节点**没有独立端口**（节点名要到运行时才知道），只能经共享端口 + selector 使用。

### 模块自己拉（要独立端口就用这个）

```go
nodes, err := mihomopool.FetchSubscription(ctx, subURL, mihomopool.FetchOptions{})
if err != nil {
    log.Fatal(err)
}
pool.Start(ctx, nodes, nil) // 每个节点分到一个独立端口
```

支持 Clash/mihomo YAML、整包 base64 包裹的 YAML、以及 `ss:// vmess:// trojan:// hysteria2:// hy2:// tuic://` 分享链接。

> **不要**把订阅 YAML 原文拼到自己的配置末尾。订阅里常自带 `mixed-port` / `socks-port`，会覆盖掉你设的端口。本库只生成骨架，订阅走 `proxy-providers`。

## 分发策略

```go
pool.SetStrategy(mihomopool.StrategyLeastInflight) // 默认：最少在飞 → 综合分
```

| 策略 | 含义 |
|---|---|
| `StrategyLeastInflight` | 最少在飞优先，同分比综合分（延迟、失败惩罚）。默认 |
| `StrategyLowestLatency` | 最低延迟优先 |
| `StrategyRoundRobin` | 轮询 |
| `StrategyRandom` | 随机 |
| `StrategyHash` | 按 `Request.HashKey` 一致性哈希，同一 key 固定落同一节点 |
| `StrategyWeighted` | 按健康分加权随机 |

节点的综合分 `Score()` 由三部分组成，都可解释：

- 延迟（未测速按 500ms 计）
- 失败惩罚：每次失败 +800，最近 30s 内失败再 +600
- 在飞负载：每个在飞 +20

分数 ≥ 3000 判为不健康，不进入候选。连续失败 3 次的节点会被踢出候选，靠 `CheckHealth` 或时间推移恢复。

## 执行器语义

- **受控并发**：固定数量 worker 消费任务队列，不会因为提交 10 万个请求而起 10 万个 goroutine
- **单节点限流**：`PerNode` 上限到了，该节点从候选里剔除，返回"在飞已满"而不是无限排队（避免把内核连接池压垮）
- **失败换节点**：5xx 视为节点故障，换节点重试；4xx 是请求本身的问题，直接返回
- **重试放宽**：第一次失败后节点被降权，重试轮会在"健康节点为空"时回落到全部存活节点，避免只有一个节点时重试形同虚设
- **传输层复用**：同一个代理地址只建一个 `http.Transport`，连接池不碎片化

## 请求与响应

```go
type Request struct {
    Method       string            // 默认 GET
    URL          string            // 必填
    Header       map[string]string
    Body         []byte
    Timeout      time.Duration     // 默认 20s
    Retries      int               // 默认 0
    Filter       NodeFilter        // 限定参与节点
    HashKey      string            // StrategyHash 用
    AllowDirect  bool              // 全部失败时允许直连兜底
    Tag          string            // 原样带回
    SkipBody     bool              // 只取状态码
    MaxBodyBytes int64             // 默认 1 MiB
}
```

`NodeFilter` 支持 `Groups` / `Types` / `RequirePort` / `HealthyOnly` / `NamePrefix`。空串与纯空白项按"未设置"处理。

响应里的 `Node` 是实际使用的节点名，`ProxyURL` 是实际用的本地端口，`Attempts` 是尝试次数，`Err` 非 nil 即失败。

## 健康检查

```go
h, err := pool.CheckHealth(ctx, 16) // 16 并发测速
fmt.Println(h.State, h.Version, h.Nodes, h.Healthy)
for _, d := range h.Detail {
    fmt.Println(d.Node, d.DelayMS, d.OK, d.Error)
}
```

测速由内核侧发起（`GET /proxies/{name}/delay`），出口就是该节点，不占本地连接数。

## 跨平台

| 项 | Linux / macOS | Windows |
|---|---|---|
| 进程组 | `Setpgid`，停止时对整组发 SIGTERM | `CREATE_NEW_PROCESS_GROUP` + `CTRL_BREAK`，失败回落 Kill |
| 重载 | `SIGHUP` 发给进程组 | 无等价信号，走控制面 `PUT /configs` |
| 二进制名 | `mihomo` | `mihomo.exe` |
| 工作目录 | `os.UserCacheDir()/mihomo-pool` | 同左（回落到 `%LOCALAPPDATA%`） |
| 配置落盘 | 写临时文件再 rename | 目标被占用时先删再 rename |

二进制搜索链（`Options.Binary` 为空时）：

1. 环境变量 `MIHOMO_POOL_BINARY`
2. `<WorkDir>/bin/mihomo[.exe]`
3. `<WorkDir>/mihomo[.exe]`
4. `PATH`
5. Windows 额外尝试 `ProgramFiles/mihomo`、`Clash Verge/verge-mihomo.exe`

## 给外部程序调用：CLI + Python

除了作为 Go 库使用，本仓库还提供独立 CLI，供 Python 等语言调用。
核心是 **常驻 JSONL 协议**：一次启动、多次调用、连接与节点延迟表跨请求复用。

```bash
go build -o /usr/local/bin/mihomo-pool ./cmd/mihomo-pool
```

### Python（推荐）

```python
import asyncio
from mihomo_pool import Pool

async def main():
    async with Pool(binary_mihomo="/usr/local/bin/mihomo",
                    nodes_file="nodes.json",
                    workers=64, per_node=8) as pool:
        results = await pool.map([f"https://e.com/{i}" for i in range(500)])
        print(sum(1 for r in results if r.ok), "/", len(results))

asyncio.run(main())
```

只依赖 Python 标准库。完整文档见 [python/README.md](python/README.md)，
示例见 `python/examples/`。

### 子命令

| 命令 | 用途 |
|---|---|
| `serve` | 常驻 JSONL 服务（Python 主用） |
| `call` | 单次请求 |
| `batch` | 批量并发（stdin 支持 JSON 数组 / JSONL / 纯 URL 行） |
| `start` | 启动内核并常驻，输出端口分配 |
| `status` / `nodes` / `health` | 查状态、列节点、并发测速 |
| `stop` | 按 PID 文件停掉托管进程 |

```bash
mihomo-pool call -nodes nodes.json https://example.com
cat urls.txt | mihomo-pool batch -nodes nodes.json -workers 64 -per-node 8
mihomo-pool status
```

全局参数放在子命令前后都支持：

```bash
mihomo-pool -bin /usr/local/bin/mihomo status
mihomo-pool status -bin /usr/local/bin/mihomo
```

### 三层并发控制

| 参数 | 位置 | 作用 |
|---|---|---|
| `workers` | Go | 同时执行的请求总数上限 |
| `per_node` | Go | 单节点同时处理数上限 |
| `max_inflight` | Go | 协议层背压，满了暂停读 stdin |
| `concurrency` | Python | `map()` 里同时在飞的协程数 |

协议细节（JSONL 格式、`body_b64`、`kind` 取值、版本协商）见
[python/README.md](python/README.md) 的「协议细节」一节。

## 真实机场订阅的坑（已处理）

用真实订阅（46 节点，31 vless + 15 hysteria2）跑出来的两个必然踩的坑：

### 1. 机场会塞"信息节点"

```
剩余流量：878.1 GB
距离下次重置剩余：22 天
套餐到期：2026-12-12
```

它们和某个真节点**连接参数完全相同**（同 server、同 port、同 uuid），只是换了名字。
当成节点用会：白占端口、测速后可能排到前面抢走真节点流量、让节点数失真。

模块默认按连接指纹去重 + 按关键词识别，`ParseSubscription` 输出 40 个而非 46 个。
需要保留时：

```go
mihomopool.ParseSubscriptionWithOptions(body, mihomopool.ParseOptions{
    KeepInfoNodes:  true,
    KeepDuplicates: true,
})
```

CLI 对应 `-keep-info-nodes` / `-keep-duplicates`。

### 2. 中文节点名导致 listener 名冲突

真实节点名几乎全是 `🇭🇰香港高速01|BGP|CMCU` 这种。早期实现把非 ASCII 字符全替换成 `_`，
结果所有国家的节点名都退化成 `out-______01_CTCU_0_1x`，内核直接：

```
level=fatal msg="Parse config error: listener out-______01_CTCU_0_1x is the duplicate name"
```

现在 listener 名追加稳定哈希后缀（`out-______01_CTCU_0_1x-3f2a91b4`），保证唯一且重载后不变。

## 测试

```bash
go vet ./...
go test -race -count=1 ./...

# 集成测试需要真实内核；找不到会自动 skip
# 强制要求跑通（CI 里用）：
MIHOMO_POOL_INTEGRATION=1 go test -race -run TestIntegration -v ./...
```

集成测试做两件事：真起内核验证状态/端口/停止；用两个假 SOCKS5 上游验证**端口隔离**——20 个并发请求全指定 node0，node1 必须命中 0 次。

## 已知边界

- `listeners[n].proxy` 需要 mihomo v1.18+，集成测试在 v1.19.34 上验证通过
- provider 订阅节点没有独立端口，高并发下只能走共享端口 + selector，存在全局争用
- `Options.BasePort` 到 `BasePort+N-1` 必须全部空闲；本库不自动探测，冲突会让内核启动失败并在错误里带日志尾部
- DNS 段给了一份 fake-ip 的保守默认，机场自带的 DNS 配置不会生效（本库只写 `proxy-providers`，不整包拷贝订阅）
