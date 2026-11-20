# Python 调用指南

目标：让 Python 以最少的心智负担，吃到 Go 侧的**真异步高并发**。

## 为什么不是纯 Python 客户端

- Python 侧 `requests`/`aiohttp` 的并发受 GIL 与协程调度限制，几千并发就开始掉速
- 每次调用都重新起 mihomo 内核要付 100ms~2s 的启动代价
- 连接池、TLS 会话、节点延迟表没法跨请求复用

所以这里的设计是：**一个常驻的 `mihomo-pool serve` 子进程 + JSONL 协议**。
Python 只负责投递任务和收结果，并发由 Go 的 worker 池执行。

```
Python                          Go 常驻进程                    mihomo 内核
──────                          ──────────                     ──────────
pool.get(url)  ────JSON行──────►  分发器 ──选节点──►  节点A:17900
                                   │                   节点B:17901
               ◄───JSON行───────   worker池         节点C:17902
```

## 安装

只需要一个二进制 + 一个 Python 包（纯标准库，无第三方依赖）：

```bash
# 1. 编译 CLI
cd /path/to/mihomo-pool
go build -o /usr/local/bin/mihomo-pool ./cmd/mihomo-pool

# 2. 让 Python 能 import
export PYTHONPATH=/path/to/mihomo-pool/python:$PYTHONPATH
# 或者把 python/mihomo_pool 拷到你的项目里
```

Python 客户端按这个顺序找二进制：

1. `Pool(binary=...)` 显式传入
2. 环境变量 `MIHOMO_POOL_BIN`
3. 包同目录
4. `PATH`

## 异步用法（推荐）

```python
import asyncio
from mihomo_pool import Pool

async def main():
    pool = Pool(
        binary_mihomo="/usr/local/bin/mihomo",
        work_dir="/var/tmp/mihomo-pool",
        workers=64,        # Go 侧并发 worker 数（真正的并发度）
        per_node=8,        # 单节点最大在飞请求数
        nodes_file="nodes.json",
    )
    await pool.start()
    try:
        print(pool.hello)        # {"protocol":1,"state":"running","nodes":3,...}

        # 单个请求
        r = await pool.get("https://example.com/")
        print(r.status, r.node, r.duration_ms)

        # 200 个请求并发跑
        urls = [f"https://example.com/{i}" for i in range(200)]
        results = await pool.gather([pool.get(u) for u in urls])
        ok = sum(1 for r in results if r.ok)
        print(f"{ok}/{len(results)} 成功")

        # 限制 Python 侧同时在飞的协程数（与 Go 侧并发解耦）
        results = await pool.map(urls, concurrency=50)
    finally:
        await pool.aclose()

asyncio.run(main())
```

`async with` 也行：

```python
async with Pool(binary_mihomo="/usr/local/bin/mihomo", nodes_file="nodes.json") as pool:
    r = await pool.get("https://example.com")
```

## 同步用法

```python
from mihomo_pool import Pool

with Pool(binary_mihomo="/usr/local/bin/mihomo", nodes_file="nodes.json") as pool:
    r = pool.get_sync("https://example.com")
    print(r.text)
```

同步上下文内部起一个事件循环线程，退出时自动收尾。

## 请求参数

```python
await pool.request(
    "https://api.example.com/v1/items",
    method="POST",
    headers={"Authorization": "Bearer xxx"},
    json_body={"name": "test"},          # 自动设 Content-Type
    body=b"\x00\x01",                    # 或者传 bytes（走 base64）
    timeout_ms=15000,
    retries=2,                           # 失败换节点重试次数
    tag="task-1",                        # 原样带回，便于对账
    hash_key="user-42",                  # 一致性哈希：同一 key 固定落同一节点
    allow_direct=False,                  # 所有节点失败时是否直连兜底
    max_body_bytes=2 << 20,
    node_filter=NodeFilter(groups=["hk"], healthy_only=True),
    check=True,                          # 非 2xx 抛异常
)
```

响应对象：

| 属性 | 含义 |
|---|---|
| `ok` | 请求是否成功（网络层 + 2xx/3xx） |
| `status` | HTTP 状态码 |
| `body` / `text` / `json()` | 正文（bytes / str / 解析后） |
| `header` / `header_get(name)` | 响应头 |
| `node` | **实际使用了哪个节点** |
| `proxy_url` | 实际使用的本地端口 |
| `attempts` | 尝试次数 |
| `duration_ms` | 总耗时 |
| `error` | 失败原因 |

## 节点筛选

```python
from mihomo_pool import NodeFilter

# 只用香港节点，且必须健康、有独立端口
await pool.get(url, node_filter=NodeFilter(
    groups=["hk"], types=["ss", "trojan"],
    name_prefix="hk-", require_port=True, healthy_only=True,
))
```

## 分发策略

```python
await pool.set_strategy("least-inflight")   # 默认：最少在飞
# lowest-latency / round-robin / random / hash / weighted
```

高并发下推荐 `least-inflight`（默认）或 `hash`（需要会话粘性时）。

## 控制面

```python
await pool.status()                # 内核状态、节点数、端口数
await pool.nodes()                 # 节点列表（含 port / proxy_url / score）
await pool.health(concurrency=16)  # 并发测速，返回每个节点的延迟
await pool.stats()                 # worker 数、队列长度、在飞数
await pool.refresh()               # 从控制面重新同步节点
await pool.set_strategy("random")
await pool.ping()                  # 存活探测
```

## 单次调用（不常驻）

脚本里偶尔用一次、不想管进程生命周期：

```python
from mihomo_pool import call, batch

r = call("https://example.com", binary_mihomo="/usr/local/bin/mihomo",
         nodes_file="nodes.json", retries=2)
print(r.status, r.text)

results = batch([
    {"url": "https://example.com/a"},
    {"url": "https://example.com/b", "tag": "B"},
], workers=32, per_node=8, binary_mihomo="/usr/local/bin/mihomo",
   nodes_file="nodes.json")
```

`call`/`batch` 每次起一个短命进程，适合低频场景。高频请用 `Pool`。

## 并发模型

三个参数决定并发，别混：

| 参数 | 位置 | 含义 |
|---|---|---|
| `workers` | Go 侧 | 同时执行的请求数上限 |
| `per_node` | Go 侧 | 单个节点同时处理的请求数上限 |
| `concurrency` | Python 侧 | `pool.map()` 里同时在飞的协程数 |

`pool.gather()` 不做 Python 侧限流——它一次把所有协程都投递出去，
真正的并发由 Go 的 worker 池卡住。所以要控总量，用 `pool.map(concurrency=N)`
或在 Go 侧调小 `workers`。

**背压**：Go 侧有 `max_inflight`（默认 `workers*8`），在途请求满了就暂停读 stdin，
Python 的写入会被管道阻塞，不会把内存打爆。

## 协议细节

每个方向一行 JSON。请求：

```json
{"id":"1","op":"request","url":"https://e.com","method":"GET","header":{"X":"1"},"timeout_ms":20000,"retries":2}
```

响应：

```json
{"id":"1","op":"request","kind":"resp","ok":true,"status":200,"body_b64":"...","node":"hk-1","attempts":1,"duration_ms":37}
```

- `kind` 取值：`hello`（握手）/`resp`（成功）/`err`（失败）/`ack`（控制类回执）
- `body` 一律 base64，二进制安全
- 未知字段会被忽略，客户端可以只读关心的字段
- 握手里的 `protocol` 必须等于客户端常量 `PROTOCOL_VERSION`，否则抛 `ProtocolVersionMismatch`

## 直接调 CLI

不想用 Python 包、或者要在 shell 里用：

```bash
# 常驻服务（自己拼 JSON 行）
echo '{"id":"1","op":"request","url":"https://example.com"}' | mihomo-pool serve -nodes nodes.json

# 单次
mihomo-pool call -nodes nodes.json https://example.com

# 批量（stdin 可以是 JSON 数组、JSONL、或纯 URL 行）
cat urls.txt | mihomo-pool batch -nodes nodes.json -workers 64 -per-node 8 > out.jsonl

# 流式：边完成边输出
cat urls.txt | mihomo-pool batch -stream -nodes nodes.json

# 查状态
mihomo-pool status
mihomo-pool nodes
mihomo-pool health -concurrency 32
```

全局参数放在子命令前后都行：

```bash
mihomo-pool -bin /usr/local/bin/mihomo -dir /tmp/w status
mihomo-pool status -bin /usr/local/bin/mihomo -dir /tmp/w
```

## nodes.json 格式

```json
[
  {"name": "hk-1", "type": "ss", "server": "1.2.3.4", "port": 8388,
   "group": "hk", "raw": {"cipher": "aes-256-gcm", "password": "xxx"}},
  {"name": "us-1", "type": "trojan", "server": "5.6.7.8", "port": 443,
   "group": "us", "raw": {"password": "yyy", "sni": "a.example.com"}}
]
```

也可以不给 `nodes`，直接给订阅地址 `subscription="https://..."`（或 CLI 的 `-sub`），
由模块负责拉取和解析。

## 真实订阅的自动清洗

真实机场的订阅几乎都会塞两类脏数据，模块默认处理：

| 情况 | 处理 | 关闭方式 |
|---|---|---|
| 信息节点（剩余流量、套餐到期、重置天数…） | 按关键词识别后丢弃 | `Pool(..., keep_info_nodes=True)` |
| 连接参数完全相同的重复条目 | 按 server+port+uuid 指纹去重，保留首条 | `Pool(..., keep_duplicates=True)` |

实测某机场 46 条 → 清洗后 40 条。

## 常见问题

**Q: 提示"全部节点在飞已满"**

单节点 `per_node` 上限到了。要么调大 `per_node`，要么增加节点。
这是**主动背压**而不是错误——继续堆积只会让内核连接池崩溃。

**Q: 请求落到了 `PASS-RULE` / `DIRECT` 这类名字上**

这些是 mihomo 内置出站，不是节点。库层已经过滤，如果还遇到说明版本差异，
把 `pool.nodes()` 的输出发出来定位。

**Q: 想要每个请求走固定节点**

```python
await pool.get(url, node_filter=NodeFilter(name_prefix="hk-1"))
```

或者用 `hash_key`（配合 `set_strategy("hash")`）实现会话粘性。

**Q: 进程没退出，卡住了**

`Pool` 一定要 `await pool.aclose()` 或用 `async with`。脚本异常退出时子进程可能残留，
用 `pkill -f "mihomo-pool serve"` 清理。
