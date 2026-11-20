# Changelog

本文件记录所有值得注意的变更。

格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## 版本策略

| 变更类型 | 版本位 | 例子 |
|---|---|---|
| 不兼容的 API 改动 | MAJOR | 改了 `Pool` / `Request` 的字段或语义 |
| 向后兼容的新功能 | MINOR | 新增子命令、新增选项、新增分发策略 |
| 向后兼容的修复 | PATCH | 修 bug、加防护、改文档 |

**两套版本号，别混：**

- **发行版本**（git tag，如 `v1.0.0`）：打进二进制，`mihomo-pool version` 的 `cli_version`
- **库 API 版本**（`options.go` 里的 `Version` 常量）：`lib_version`，只在不兼容改动时跳

发版由 GoReleaser 自动完成，见 [`.goreleaser.yaml`](.goreleaser.yaml)。
变更日志的发布段由 GitHub Release 自动生成（只看 `feat` / `fix` / `perf` 提交），
本文件用于人工维护「升级要注意什么」。

## [Unreleased]

## [1.2.0] - 2026-11-20

### 新增

- **订阅池**：多订阅统一编排、订阅级健康与熔断、权重配额、细粒度标签
  - 订阅级隔离：一个订阅失败不影响其他订阅的节点继续服务
  - 熔断：连续失败 `MaxConsecutiveFail` 次暂停派活，`CircuitHalfOpen` 后渐进恢复；
    熔断不清空已有节点
  - 动态权重：静态权重 × 优先级 × 成功率 × 连续失败惩罚 × 延迟衰减
  - 配额：限制单订阅最大在飞占比，防止一个订阅吃掉全部并发；
    全部超配额时退回原候选，不让请求无节点可用
  - 标签：从节点名自动提取 `region:` / `rate:` / `stream` / `dedicated` /
    `bgp` / `speed` / `proto:` / `expiry`
  - 跨订阅重名自动消歧（`name@sub`）
  - CLI：`subpool list | refresh | nodes | check`

### 说明

- **跨订阅去重默认关闭**：同一物理节点出现在不同订阅时保留为独立记录。
  两个订阅的额度、限速、到期时间各自独立，合并后无法追踪消耗归属。
  需要合并时显式开 `DedupeAcrossSubs` / `-dedupe`。

### 修复

- `attachMeta` 用原始节点名建索引，跨订阅重名时归属串台，
  导致健康统计互相污染（实测第二个订阅显示 80 个健康节点而其只有 40 个）
- `subpool` 子命令不识别子命令前的全局参数（`-bin` 被当成动作）
- 地区关键词把容量单位 `GB`（吉字节）误判为英国
- 公告节点关键词缺 `总量`/`已用` 等措辞，导致漏判

## [1.1.0] - 2026-11-20

### 新增

- **内嵌内核构建（`-tags embedded`）**：把 mihomo 与 geodata 一起编译进二进制，
  运行时不依赖外部 mihomo。产物 58–61 MB，启动 17 ms，单文件可分发。
  lite 与 full 共用同一份代码，`version` 输出里的 `build` 字段区分。
- 发布页同时提供两种产物：`mihomo-pool_*`（lite）与 `mihomo-pool-embedded_*`（full）
- 内核后端抽象（`backend` 接口）：`processBackend` / `embedBackend` 可切换，
  上层 Pool / Executor / CLI 无感
- CI 增加 embedded 任务：vet、测试、构建、无外部内核的端到端验证

### 变更

- **许可证 MIT → GPL-3.0**：内嵌版链接 mihomo（GPL-3.0）与 meta-rules-dat 的
  geodata（GPL-3.0），整体按 GPL-3.0 发布；lite 版为一致性同样采用 GPL-3.0

### 文档

- README 增加 [mihomo 仓库链接](https://github.com/MetaCubeX/mihomo) 与两种构建的对比表
- 新增内嵌实现的技术要点（geodata、日志重定向、配置对象、applyConfig、关闭顺序）

### 已知限制

- 内嵌版编译时间显著变长（170 个依赖 × 6 平台），CI 用 Go module 缓存缓解
- 内嵌版崩溃会带走主进程，无自动重启（`processBackend` 有）


## [1.0.0] - 2026-11-20

首个版本。

### 新增

- **Go 库**：`Pool` 编排 mihomo 内核，每节点独占一个本地端口（`listeners[n].proxy`），
  高并发下节点之间互不干扰；六种分发策略、节点健康分、并发测速
- **CLI**：`serve`（常驻 JSONL 流）/ `call` / `batch` / `start` / `status` /
  `nodes` / `health` / `stop` / `version`
- **Python 客户端**：纯标准库，`Pool` 支持 `async with` 与 `with` 两种上下文，
  `gather` / `map` 两种批量方式
- **订阅支持**：Clash/mihomo YAML、整包 base64、`ss` / `vmess` / `trojan` /
  `hysteria2` / `tuic` 分享链接
- **跨平台**：linux / darwin / windows × amd64 / arm64，共 6 个平台
- **技能说明**：`skills/mihomo-pool/SKILL.md`，给 AI 助手用的完整使用指南

### 修复

发布前用真实机场订阅（46 节点）测试后修复的硬问题：

- **`PASS-RULE` 被当成节点**：mihomo 内置出站类型是 `PassRule` 而非 `Pass`，
  名单漏了它。实测 200 并发里有 125 个请求走错了不可控的规则出口
- **中文节点名导致 listener 重名，内核 fatal 退出**：非 ASCII 字符全替换成 `_` 后
  所有国家的节点名退化成一个，内核报 `listener ... is the duplicate name`
- **机场信息节点被当成真节点**：`剩余流量` / `套餐到期` / `重置天数` 三类条目与
  真节点的 server/port/uuid 完全相同，白占端口且抢流量
- **`batch` 绕过 worker 池**：自己起 goroutine 撞上单节点在飞上限，产生大量假失败
- **`batch` 输入不带 `id` 时结果互相覆盖**：20 条只回填 1 条
- **`Configure` 关闭仍被使用的队列**：`send on closed channel` panic
- **`ForEach` 写已关闭通道**：同上
- **`Status()` 死锁**：`sync.Mutex` 不可重入，持锁时二次加锁
- **stdout 接 `io.Writer` 导致内核起不来**：`os/exec` 插管道 copier，上游关闭即 broken pipe
- **5xx 被当成功**：重试与直连兜底都不触发
- **`Groups: []string{""}` 静默筛掉所有节点**

### 内嵌实现踩到的坑（已修）

- `executor.ApplyConfig` 不启动 external-controller，必须用 `hub.ApplyConfig`
- `executor.Shutdown()` 只关 TUN，入站监听器需 `PatchInboundListeners(空map, dropOld=true)`
  + `ReCreateMixed(0)` 显式关闭，否则端口泄漏、重启撞 `address already in use`
- mihomo `log` 包 init 里 `logrus.SetOutput(os.Stdout)`，内嵌会污染 CLI 的 JSONL 协议
- 拼接订阅 YAML 改配置会撞重复键（mixed-port 等），必须改 `RawConfig` 对象
- 缺 geodata 时 GEOIP 规则让 `ParseRawConfig` 直接失败（`can't download MMDB`）

### 已知限制

- `Options.BasePort` 到 `BasePort+N-1` 不自动探测端口冲突，撞端口会让内核启动失败
  （错误信息会带日志尾部）
- provider 订阅节点没有独立端口，只能经共享端口 + selector 使用，存在全局争用
- `stop` 依赖 `start -ready-file` 写下的 PID 文件；内核是独立进程，本进程无法停
  别处起的内核
- 真实机场订阅无法进入公开 CI，真实节点验证只能人工跑

[Unreleased]: https://github.com/Facetomyself/mihomo-pool/compare/v1.2.0...HEAD
[1.2.0]: https://github.com/Facetomyself/mihomo-pool/releases/tag/v1.2.0
[1.1.0]: https://github.com/Facetomyself/mihomo-pool/releases/tag/v1.1.0
[1.0.0]: https://github.com/Facetomyself/mihomo-pool/releases/tag/v1.0.0
