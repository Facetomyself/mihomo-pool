# 发布流程

发版完全由 GoReleaser + GitHub Actions 完成，本地只需要打一个标签。

## 一次发版要做什么

```bash
# 1. 确认主干干净且 CI 绿
git status -sb
gh run list --limit 3

# 2. 更新 CHANGELOG.md：把 [Unreleased] 的内容挪到新版本段
#    新增一条：## [1.1.0] - 2026-12-01

# 3. 提交
git add CHANGELOG.md
git commit -m "docs(changelog): 准备 v1.1.0"

# 4. 打标签并推送（这一步触发发布）
git tag v1.1.0
git push origin main
git push origin v1.1.0
```

推完标签后 `.github/workflows/release.yml` 会自动：

1. **verify** — gofmt、vet、`-race` 测试、Python 测试
2. **goreleaser** — 构建 6 平台产物、生成 checksums、建 GitHub Release
3. **smoke** — 下载 linux/amd64 产物，校验 sha256、执行 `version`、比对版本号与 tag、装内核跑一次真实请求

三步全绿才算发布成功。任何一步失败都不会产出 Release（`mode: replace` 也会覆盖半成品）。

## 版本号怎么定

| 变更 | 位 | 例子 |
|---|---|---|
| 不兼容的 API 改动 | MAJOR | `Request` 字段改名、`Pool` 方法语义变化 |
| 向后兼容的新功能 | MINOR | 新子命令、新选项、新策略 |
| 向后兼容的修复 | PATCH | 修 bug |

预发布用后缀：`v1.1.0-rc1`、`v1.1.0-beta2`。
GoReleaser 的 `prerelease: auto` 会自动标成预发布，不会覆盖 latest。

**两个版本号别混：**

- `vX.Y.Z`（git tag）→ 注入到 `main.buildVersion` → `mihomo-pool version` 的 `cli_version`
- `options.go` 里的 `Version` 常量 → `lib_version`，只在不兼容改动时跳

`lib_version` 是给 Go 库用户的 API 兼容信号；`cli_version` 是发行版本。
两者都在 `version` 子命令里输出，脚本可以据此判断兼容性。

## 产物清单

| 文件 | 平台 |
|---|---|
| `mihomo-pool_Linux_x86_64.tar.gz` | linux/amd64 |
| `mihomo-pool_Linux_arm64.tar.gz` | linux/arm64 |
| `mihomo-pool_Darwin_x86_64.tar.gz` | darwin/amd64 |
| `mihomo-pool_Darwin_arm64.tar.gz` | darwin/arm64 |
| `mihomo-pool_Windows_x86_64.zip` | windows/amd64 |
| `mihomo-pool_Windows_arm64.zip` | windows/arm64 |
| `checksums.txt` | 全部产物的 sha256 |

每个包里含：`mihomo-pool`、`mihomo-pool-demo`、`README.md`、`CHANGELOG.md`、
`LICENSE`、`skills/`、`python/`（只含源码，不含 `__pycache__`）。

## 本地验证（不发版）

改完 `.goreleaser.yaml` 先本地跑一遍：

```bash
# 只校验配置语法
goreleaser check

# 完整构建全部平台，产物在 dist/，但不发布
goreleaser release --snapshot --clean

# 看某个产物里到底有什么
tar tzf dist/mihomo-pool_Linux_x86_64.tar.gz
sha256sum -c dist/checksums.txt
```

`--snapshot` 的版本号是 `{{ incpatch .Version }}-next`，所以本地构建出来会是
`0.0.1-next` 这种，属正常。

## 打包要注意的坑（已处理，别改回去）

**别用 `python/**` 通配**

本地跑过测试后 `python/__pycache__/*.pyc` 会存在，通配会把它们打进发布包。
那些是特定解释器版本的字节码，对用户只是垃圾。现在显式列出：

```yaml
- python/mihomo_pool/*.py
- python/README.md
- python/examples/*.py
- python/tests/*.py
```

**`CGO_ENABLED=0` 必须有**

交叉编译到非本机架构时，cgo 会去找目标平台的 C 编译器，找不到就失败。
本项目二进制不依赖 cgo（mihomo 是独立进程，不链接它）。

**`-trimpath` 必须有**

否则产物里带构建机的绝对路径，既泄漏信息又让构建不可复现。

**CI 里 `-race` 只能跑原生架构**

在 x86 runner 上设 `GOARCH=arm64` + `CGO_ENABLED=1` 跑 `-race`，
链接器会去汇编 ARM 指令集直接报 `no such instruction: stp x29,x30`。

## 撤销一次发版

```bash
# 删除 Release（保留 tag）
gh release delete v1.1.0 --yes

# 删除 tag（本地 + 远端）
git tag -d v1.1.0
git push origin :refs/tags/v1.1.0

# 修完之后重新打标签
```

`mode: replace` 意味着重跑发布流程会覆盖同名 Release，不需要先手动删。

## 检查发布结果

```bash
# 看发布状态
gh release list
gh release view v1.1.0

# 验证产物可下载且校验通过
curl -sL -o pkg.tar.gz https://github.com/Facetomyself/mihomo-pool/releases/download/v1.1.0/mihomo-pool_Linux_x86_64.tar.gz
curl -sL -o sums.txt https://github.com/Facetomyself/mihomo-pool/releases/download/v1.1.0/checksums.txt
grep Linux_x86_64 sums.txt > one.sha && sha256sum -c one.sha
```
