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

### 文档

- README 增加 [mihomo 仓库链接](https://github.com/MetaCubeX/mihomo) 与依赖关系说明
- 新增"关于内嵌内核的说明"一节：实测把 mihomo 编译进来的可行性与代价
  （体积 5MB→45MB、依赖 1→170、启动 2s→17ms、但失去崩溃隔离且需自备 geodata），
  附可直接用的内嵌代码骨架与 build tag 切换思路


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

### 已知限制

- `Options.BasePort` 到 `BasePort+N-1` 不自动探测端口冲突，撞端口会让内核启动失败
  （错误信息会带日志尾部）
- provider 订阅节点没有独立端口，只能经共享端口 + selector 使用，存在全局争用
- `stop` 依赖 `start -ready-file` 写下的 PID 文件；内核是独立进程，本进程无法停
  别处起的内核
- 真实机场订阅无法进入公开 CI，真实节点验证只能人工跑

[Unreleased]: https://github.com/Facetomyself/mihomo-pool/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/Facetomyself/mihomo-pool/releases/tag/v1.0.0
