---
name: mihomo-pool
description: 用 mihomo 多节点池做异步高并发请求：每节点独立本地端口互不干扰，支持订阅/真实机场、Go 库、CLI 与 Python 客户端
whenToUse: 需要换出口 IP 做大量请求、要按地区选出口、需要并发压测/批量抓取、或要给 Python/Go 程序接入多节点代理时
role: recon,vuln-scan,exploit
enabled: true
---

# mihomo-pool：多节点独立端口池 + 异步高并发请求

仓库：`https://github.com/Facetomyself/mihomo-pool`（MIT）

## 什么时候用它，什么时候别用

**用**：目标有 WAF / 已出现 429 或 403 激增 / 需要大量请求分摊到多个出口 IP / 需要按地区选出口（只要日本节点）/ Python 或 Go 程序要批量抓取。

**别用**：

- 只需要一次性换个 IP → `curl --proxy` 一条命令就够，见技能 `cn-proxy-pool`
- 要进内网 → 那是隧道，见 `tunnel-selection`、`suo5-tunnel`、`frp-tunnel`
- 免费 HTTP 代理池（非 mihomo 订阅） → 见 `cn-proxy-pool`

## 核心机制：为什么不是"单端口切节点"

mihomo 的 `mixed-port` 只有一套规则和一个 `GLOBAL` selector。用 `PUT /proxies/GLOBAL` 切节点是**全局状态**：请求 A 正在用香港节点，请求 B 一进来切到日本，A 的连接被连带影响。高并发下这种争用直接毁掉吞吐。

本工具的做法是**一个内核进程、每节点一条 listener**：

```yaml
listeners:
  - {name: out-hk-1, type: mixed, port: 17900, listen: 127.0.0.1, proxy: hk-1}
  - {name: out-us-1, type: mixed, port: 17901, listen: 127.0.0.1, proxy: us-1}
```

N 个节点 = N 个本地端口。`socks5://127.0.0.1:17900` 永远是 `hk-1`，互不牵扯。分发器只往对应端口发请求，不碰控制面状态。

## 安装

```bash
# 1. mihomo 内核（必须有）
curl -sL -o /tmp/mihomo.gz https://github.com/MetaCubeX/mihomo/releases/download/v1.19.34/mihomo-linux-amd64-compatible-v1.19.34.gz
gunzip -f /tmp/mihomo.gz && chmod +x /tmp/mihomo && sudo mv /tmp/mihomo /usr/local/bin/mihomo
mihomo -v    # Mihomo Meta v1.19.34

# 2. 本工具（CLI + Go 库同一个仓库）
git clone https://github.com/Facetomyself/mihomo-pool
cd mihomo-pool
go build -o /usr/local/bin/mihomo-pool ./cmd/mihomo-pool

# 3. Python 客户端（纯标准库，无第三方依赖）
export PYTHONPATH=/path/to/mihomo-pool/python:$PYTHONPATH
```

## 场景一：Python 批量请求（主用）

一次启动、多次调用。内核启动要 100ms~2s，连接池与节点延迟表跨请求复用，所以别每次重起。

```python
import asyncio
from mihomo_pool import Pool

async def main():
    async with Pool(
        binary_mihomo="/usr/local/bin/mihomo",
        subscription="https://airport.example/api/v1/client/subscribe?token=xxx",
        workers=64,      # Go 侧总并发
        per_node=4,      # 单节点在飞上限（真机场单节点扛不住太多）
        strategy="least-inflight",
    ) as pool:
        print(pool.hello)   # 节点数、内核版本、默认出口

        # 500 个请求并发，失败自动换节点重试
        urls = [f"https://target.example.com/item/{i}" for i in range(500)]
        results = await pool.map(urls, retries=2, timeout_ms=20000)

        ok = sum(1 for r in results if r.ok)
        print(f"{ok}/{len(results)}")
        for r in results[:3]:
            print(r.status, r.node, r.duration_ms)   # node = 实际用的节点

asyncio.run(main())
```

**四层并发参数，别混：**

| 参数 | 位置 | 作用 |
|---|---|---|
| `workers` | Go | 总并发上限 |
| `per_node` | Go | 单节点并发上限 |
| `max_inflight` | Go | 协议层背压，满了暂停读 stdin |
| `concurrency` | Python | `map()` 里同时在飞的协程数 |

**按地区选出口：**

```python
from mihomo_pool import NodeFilter

# 只要日本节点
await pool.get(url, node_filter=NodeFilter(name_prefix="🇯🇵"))

# 要健康且有独立端口的
await pool.get(url, node_filter=NodeFilter(
    groups=["hk"], healthy_only=True, require_port=True))
```

**会话粘性**（同一用户固定走同一节点）：

```python
await pool.set_strategy("hash")
await pool.get(url, hash_key="user-42")
```

## 场景二：CLI 批量

```bash
# 单次
mihomo-pool call -sub "https://airport.example/..." -timeout 20000 https://target.example.com/

# 批量（stdin 支持 JSON 数组 / JSONL / 纯 URL 行）
cat urls.txt | mihomo-pool batch \
  -sub "https://airport.example/..." \
  -workers 64 -per-node 4 -timeout 20000 > out.jsonl

# 流式，边完成边出结果
cat urls.txt | mihomo-pool batch -stream -sub "..." -workers 64
```

全局参数放子命令前后都行：

```bash
mihomo-pool -bin /usr/local/bin/mihomo status
mihomo-pool status -bin /usr/local/bin/mihomo
```

## 场景三：Go 库

```go
pool, _ := mihomopool.New(mihomopool.DefaultOptions())
defer pool.Stop(ctx)

nodes, _ := mihomopool.FetchSubscription(ctx, subURL, mihomopool.FetchOptions{})
pool.Start(ctx, nodes, nil)          // 每节点分到一个独立端口
pool.ConfigureExecutor(64, 4)

ch, _ := pool.Executor().RunAsync(ctx, reqs)
for resp := range ch {
    fmt.Println(resp.Node, resp.Status)   // resp.Node = 实际节点
}
```

## 常用命令

```bash
# 看一眼订阅里的节点（自己起临时内核，用完即停）
mihomo-pool nodes -sub "https://airport.example/api/v1/client/subscribe?token=xxx"

# 测速：每个节点的真实延迟
mihomo-pool health -sub "https://airport.example/..." -concurrency 16

# 长期托管内核（给别的进程用）
mihomo-pool start -sub "..." -ready-file /tmp/ready.json
mihomo-pool status
mihomo-pool stop -pid-file /tmp/ready.json
```

`nodes` / `health` 带 `-sub` 或 `-nodes` 时会自己拉起一个临时内核，跑完自动停；
不带则连已经运行的内核（`-api` 指向它的控制面端口）。

全局参数（`-bin` `-dir` `-socks` `-api` `-base`）写在子命令前后都行：

```bash
mihomo-pool -bin /usr/local/bin/mihomo health -sub "..."
mihomo-pool health -bin /usr/local/bin/mihomo -sub "..."
```

动手前先 `health` 测速：慢节点会被自动降权，不用手动挑节点。

## 真实机场的坑（工具已处理，但你得知道）

真实订阅（实测 46 节点）一跑就踩两个：

### 1. 机场塞"信息节点"

```
剩余流量：878.1 GB
距离下次重置剩余：22 天
套餐到期：2026-12-12
```

它们和某个真节点的 **server/port/uuid 完全相同**，只是换了名字。当节点用会：白占端口、测速后抢真节点流量、节点数虚高。

工具默认按连接指纹去重 + 关键词识别，46 条→40 条。要保留：

```bash
mihomo-pool nodes -sub "..." -keep-info-nodes -keep-duplicates
```

### 2. 中文节点名导致 listener 重名 → 内核 fatal

真实节点名是 `🇭🇰香港高速01|BGP|CMCU` 这种。早期实现把非 ASCII 全替换成 `_`，所有国家都退化成一个名字，内核：

```
level=fatal msg="listener out-______01_CTCU_0_1x is the duplicate name"
```

已修复（追加稳定哈希后缀）。如果你自己生成配置，**必须保证 listener 名唯一**。

## 测速与出口验证

```bash
# 测速：拿到每个节点真实延迟
mihomo-pool health -concurrency 16

# 验证出口真的不同：对 3 个节点分别查出口 IP
for p in 17900 17901 17902; do
  curl -s -x "socks5h://127.0.0.1:$p" https://api.ipify.org; echo
done
```

**别只看成功率高就认为节点池正常**——必须验证出口 IP 确实不同。同 IP 的多条节点（例如 vless 与 hysteria2 双协议入口）在机场里是正常设计，不是数据错误。

## 性能基线（真实机场实测，供参考）

| 目标 | 并发 | 结果 | 吞吐 |
|---|---|---|---|
| gstatic 204 | 100 | 100/100 | 161 req/s |
| example.com | 100 | 100/100 | 241 req/s |
| httpbin /get | 100 | 100/100 | 92 req/s |

40 个节点分散使用，单节点 4 在飞。延迟 p50 约 430ms（204）、76ms（example.com）。

## 排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `全部节点在飞已满` | `per_node` 太小 | 调大 `per_node` 或加节点。这是**主动背压**，不是错误 |
| 内核启动后立即退出 | 端口冲突 / 配置无效 | 看 `<work_dir>/mihomo.log` 尾部；错误信息会带日志摘要 |
| 请求落到 `PASS-RULE`/`DIRECT` | 内置出站被当节点 | 已过滤；若复现，把 `nodes` 输出发出来 |
| `listener ... duplicate name` | 节点名碰撞 | 用本工具（已加哈希后缀），别自己拼配置 |
| 进程残留 | 未调 `aclose()` | `pkill -f "mihomo-pool serve"` |
| 订阅拉取 522/超时 | 机场按 UA 分流或抖动 | 换 UA 重试；`FetchOptions.UserAgent` |

## 端口分配

| 端口 | 用途 |
|---|---|
| 17890 | 共享 mixed 端口（规则分流，不保证走哪个节点） |
| 17891 | external-controller（控制面 API） |
| 17900 + N | 第 N 个节点的独立端口 |

**要确定出口，必须用节点的独立端口，不要用 17890。**

## 分发策略

| 策略 | 用途 |
|---|---|
| `least-inflight`（默认） | 最少在飞优先，同分比综合分。高并发首选 |
| `lowest-latency` | 最低延迟优先 |
| `round-robin` | 轮流 |
| `random` | 随机 |
| `hash` | 按 key 固定节点（会话粘性） |
| `weighted` | 按健康分加权 |

节点的综合分 = 延迟 + 失败惩罚（每次失败 +800，30s 内再 +600）+ 在飞负载（每个 +20）。**分 ≥ 3000 判为不健康，不进入候选**。连续失败 3 次的节点会被踢出。
