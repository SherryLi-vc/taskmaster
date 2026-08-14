# TaskMaster PR 2 最终复审与整改交接

日期：2026-08-14  
复审提交：`15c1537`  
目标分支：`codex/pr2-file-store`  
对比基线：`main` (`1e11839`)

## 1. 合并裁决

**暂不批准合并。**

代码可以格式化、编译并通过现有测试，但现有测试没有完整证明 PR 2 声称的跨进程锁、原子提交故障路径和 Windows 运行时行为。当前确认有 8 个 P1 和 2 个 P2 必须在本 PR 内整改。

本轮只做架构与代码审查，没有修改实习生实现。

## 2. 已通过的验证

以下命令已由复审方在提交 `15c1537` 上独立重跑：

```text
gofmt -l cmd internal schemas
go test -count=1 ./...
go test -count=1 -race ./...
go vet ./...
go build ./...
GOOS=windows GOARCH=amd64 go build ./...
git diff --check main...HEAD
```

结果均通过。当前本机 Go 为 `go1.26.5`；没有在真实 Go 1.23 工具链上执行。Windows 只完成交叉编译，不等于 Windows 运行时验证。

## 3. 必须整改项

### P1-1 删除失败却返回成功

位置：`internal/store/store.go:279-283`

`Update` 的删除分支忽略 `os.Remove` 错误。除目标不存在外，删除失败必须向上返回，不能被报告为提交成功。补充可稳定注入删除失败的测试。

### P1-2 List 静默隐藏目录读取错误

位置：`internal/store/store.go:391-395`

读取 agent 目录失败后直接 `continue`，既不返回错误，也不增加 degraded 计数。选择并固定一种协议：返回遍历错误，或把该目录计入有明确定义的 degraded 结果；测试必须锁定行为。

### P1-3 并发验收测试没有验证共享根目录

位置：`internal/store/store_impl_test.go:1084-1192`

“100 goroutine / 10 session”测试为不同 session 创建了不同 root，并且只断言 revision 大于等于 1。应使用多个独立 `Store` 实例指向同一个 root，统计每个 session 的成功提交数，并断言各 session 最终 revision 精确等于其成功提交数。

测试中的所有 `Update` 错误都必须被收集和断言，禁止 `_ = store.Update(...)`。

### P1-4 故障注入没有覆盖原子提交阶段

位置：`internal/store/store_impl_test.go:1461-1510`

当前主要覆盖 marshal 和目录权限，不足以证明协议。需要用小型、包内注入 seam 分别覆盖：临时文件 create、write、file sync、chmod、close、atomic replace、parent sync，以及删除和锁清理失败。

每个失败用例至少断言：错误被返回、旧快照仍可读且内容不变、没有把半写文件当作正式快照、锁可以按约定恢复或残留故障明确可见。

不要为了测试扩大公共 API；注入 seam 保持包内、最小化，并避免全局可变状态导致并行测试互相污染。

### P1-5 缺少 Windows 运行时 CI

位置：仓库缺少 `.github/workflows/`；Windows 实现在 `internal/store/replace_windows.go`。

新增最小 GitHub Actions 矩阵，至少在 Ubuntu、macOS、Windows 上运行 `go test ./...`、`go vet ./...` 和 `go build ./...`；支持的平台运行 race。Windows job 必须真正执行测试，不能只交叉编译。

### P1-6 subprocess 测试没有产生跨进程竞争

位置：`internal/store/store_impl_test.go:868-1020`

现有测试只启动一个子进程；另一用例的并发仅发生在一个子进程内部。应由父测试同时启动至少两个独立 helper 进程，使它们竞争同一 root、同一 session，然后收集每个进程的成功、锁超时和其他错误，最后核对快照合法性与精确 revision。

测试必须有硬超时，失败时输出每个子进程的 stdout/stderr，避免 CI 卡死且无法诊断。

### P1-7 parent fsync 错误被吞掉

位置：`internal/store/store_lock.go:180-190`

在支持目录 fsync 的 Unix 平台上，parent directory 的 open、sync、close 错误不能全部忽略后仍返回成功。把平台差异封装在小型 helper 中：支持的平台传播错误；明确不支持的平台使用有注释、可测试的降级规则。

### P1-8 锁目录 Stat 错误可绕过 75ms 总预算

位置：`internal/store/store_lock.go:246-267`

`Stat` 失败后立即 `continue`，没有检查 deadline，也没有 sleep；持续错误或 dangling symlink 可造成忙等甚至无限循环。只把明确的瞬时 `ENOENT` 当作竞争重试，其他错误直接返回；每次重试和 sleep 前都检查剩余预算，sleep 不得超过剩余时间。

### P2-1 锁释放错误被丢弃

位置：`internal/store/store_lock.go:298-319`

`ReleaseSessionLock` 没有返回值，并忽略删除 lock file / lock dir 的错误。重构内部契约，使 Store 操作可以观察清理失败。若主操作也失败，保留主错误并组合清理上下文；若主操作成功但清理失败，不得静默报告完全成功。

补充 lock file 和 lock dir 删除失败测试。

### P2-2 corrupt 保留数在异常初始状态下仍会超过 3

位置：`internal/store/store.go:308-338`

已有备份超过 3 份时，当前逻辑只删除一份再新增一份。改为在 rename 前持续删除最旧项，直到旧备份最多剩 2 份，再新增当前 corrupt 文件；最终必须始终小于等于 3。增加“预先存在 5 份”的回归测试。

## 4. 本 PR 不阻塞但要登记的债务

以下两项不要和上述阻塞项混在一起扩大整改范围：

1. `Update` 只返回 `error`，调用方仍可在闭包中保存 `ReduceResult` 并仅在成功后使用，因此当前不是正确性阻塞；Phase 3 接 CLI 前应评审是否改成 `(domain.ReduceResult, error)`，使“提交成功后通知”的契约更直接。
2. `internal/store/store_impl_test.go` 已超过 2000 行，建议按 lock、atomic write、corruption、path safety、Store basic 分文件整理，但拆文件本身不是本次合并门槛。

风险台账另记：公共 `Commit` 当前可写入较低 revision；`context.Context` 尚未参与取消；祖先目录 symlink 与 nonce 文件同用户竞态需要在安全边界确认后决定是否加固。不得顺手扩展成 daemon、数据库或消息总线。

## 5. 实习生实施顺序

1. 先修真实错误：删除、List、parent fsync、锁 Stat deadline、锁释放、corrupt retention。
2. 再建立最小故障注入 seam，并补齐各阶段测试。
3. 重写共享 root 并发测试与真正的多进程竞争测试。
4. 加入 Windows 运行时 CI。
5. 最后运行全部门禁并提交一份逐项对照报告。

不要进入 PR 3，不要实现 CLI 的 exit 0 策略，不要引入新运行时依赖，不要修改 Reducer 语义。

## 6. 完成门禁

```text
gofmt -w <本次修改的 Go 文件>
go test -count=1 ./...
go test -count=1 -race ./...
go vet ./...
go build ./...
GOOS=windows GOARCH=amd64 go build ./...
git diff --check main...HEAD
```

此外必须提供 GitHub Actions 中 Ubuntu、macOS、Windows 的真实运行结果。若 Windows job 失败，不得用本机交叉编译结果替代。

交付报告必须逐项列出：整改编号、修改文件、行为变化、对应测试名、真实命令输出、commit hash、`git status --short`、剩余阻塞。不得只写“11 findings addressed”。

## 7. 可直接复制给实习生的纯文本提示词

```text
你负责整改 TaskMaster PR 2（File Store），目标提交基于 15c1537。只写代码和测试，不重新设计架构，不进入 PR 3。

先完整阅读 TaskMaster.md 的 §8、§12、§13，以及 docs/handoffs/2026-08-14-pr2-final-rereview-handoff.md。按交接文档第 3 节逐项修复 8 个 P1 和 2 个 P2：

1. Update 删除失败必须返回错误，只有 not-exist 可按幂等成功处理。
2. List 不得静默跳过 agent 目录读取错误；固定并测试 error 或 degraded 协议。
3. 100 goroutine / 10 session 测试必须让独立 Store 实例共享同一 root，逐 session 统计成功提交并精确断言 revision；不得忽略 Update 错误。
4. 用最小包内 seam 注入 create/write/file-sync/chmod/close/replace/parent-sync/delete/lock-cleanup 失败，验证旧快照不变、错误可见、无半提交。
5. 新增 Ubuntu/macOS/Windows GitHub Actions 运行时测试；Windows 不能只交叉编译。
6. subprocess 测试必须同时启动至少两个独立进程竞争同一 root 和 session，并设置硬超时、收集 stdout/stderr、核对最终 revision。
7. 支持目录 fsync 的平台必须传播 parent open/sync/close 错误；不支持的平台要有明确、可测试的降级。
8. 锁目录 Stat 失败路径必须受 75ms 总 deadline 约束；只重试明确瞬时错误，禁止忙等或无限循环。
9. 锁释放失败不得被静默吞掉；主错误与清理错误要按清晰契约返回，并补失败测试。
10. corrupt 备份无论初始有多少，处理后每会话都不得超过 3 份；覆盖预存 5 份场景。

约束：
- 单二进制、零 daemon、无 SQLite/HTTP/Redis。
- 不实现 CLI exit 0；那是 PR 3 职责。
- 不修改 Reducer 语义，不引入新的运行时第三方依赖。
- 测试 seam 尽量包内且实例化，避免扩大公共 API和全局可变状态。
- 不要以拆分 2000 行测试文件代替功能整改；拆分可在功能全绿后做。
- 不要偷偷下载或切换 Go 工具链。

完成后依次运行：gofmt、go test -count=1 ./...、go test -count=1 -race ./...、go vet ./...、go build ./...、Windows 交叉编译、git diff --check，并等待三平台 CI 全绿。

最终报告必须逐项对应 1-10，给出修改文件、行为变化、测试名和真实输出；附 commit hash、git status --short、三平台 CI 链接或状态、剩余阻塞。任何一项未完成都明确写出，不要概括为“all findings addressed”。
```

## 8. 审查覆盖说明

- 代码审查覆盖：correctness、reliability、API contract、testing、security、maintainability。
- 外部对抗审查尝试：route `claude`，requested model `opus`，requested effort `high`；执行环境发生网络/API 解析失败，没有产出可用审查结果，因此没有把它计作独立佐证。
- 未发现仓库级 `AGENTS.md` 或 `CLAUDE.md` 约束文件。
- 本轮最终问题均由本地代码证据与 TaskMaster.md 验收条款支持。
