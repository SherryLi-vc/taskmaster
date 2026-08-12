---
artifact_contract: "ce-handoff/v1"
created_at: "2026-08-12T13:03:26Z"
title: "TaskMaster PR 1 实习生开发交接"
summary: "交接 TaskMaster Domain + Reducer 的实现范围、权威文档、验证门槛与本机环境注意事项。"
keywords: ["taskmaster", "pr1", "domain", "reducer", "tdd", "claude-code", "cc-switch"]
cwd: "/Users/vizhangkk/Documents/ChatGPT/TaskMaster"
resume_focus: "由实习生按 TDD 完成 PR 1；架构负责人只审方案和产出，不代写业务代码。"
repository: "TaskMaster"
repo_root_sha: "205f0fa"
branch: "main"
head: "205f0fa"
---

# TaskMaster PR 1 实习生开发交接

## 1. 交接目标

由实习生完成仓库最小可编译骨架，以及 PR 1：Domain + Reducer。

架构负责人只负责方案、边界澄清和 Review，不负责代写具体业务代码。实习生应自行编写测试与实现，并保留 RED → GREEN 的执行证据。

## 2. 当前真实状态

已完成：

- 技术架构已批准并提交，commit：`205f0fa`。
- PR 1 逐步实施计划已经生成。
- Event 协议已补齐必填 `capability`，避免 Reducer 根据事件来源猜测能力等级。
- 没有创建功能分支或 worktree。
- 没有创建 Go 源码、测试或 Schema 文件。
- 没有安装依赖或修改 Claude Code、CC Switch 配置。

本机注意事项：

- 当前 shell 的 PATH 中没有 `go` 命令。实习生开始前需安装 Go 1.23 或更高版本，并记录 `go version`。
- 根目录有一个未跟踪的 `.DS_Store`，不要暂存或提交。
- 当前分支为 `main`。禁止直接在 main 实现，应创建 `codex/pr1-domain-reducer` 或团队约定的功能分支；如使用 worktree，先确保 `.worktrees/` 已加入 `.gitignore`。

## 3. 权威材料与读取顺序

1. `TaskMaster.md`
   - 产品边界、状态协议、架构、错误策略和最终验收标准。
   - 任何实现与本文冲突时，以它为产品/架构真相。
2. `docs/superpowers/plans/2026-08-12-pr1-domain-reducer.md`
   - PR 1 的文件清单、接口、TDD 步骤、命令和提交拆分。
   - 逐项勾选执行，不要跳过 RED。
3. `prompts/pr1-intern-implementation.txt`
   - 可直接复制给 Coding Agent 的纯文本工作指令。

不要依赖当前对话历史。上述三份文件必须足以恢复上下文。

## 4. PR 1 严格范围

必须交付：

- `go.mod`，Go 1.23+。
- `internal/domain`：Status、EventKind、Source、Capability、Event、SessionSnapshot、Transition、ReduceResult、校验、TTL、哈希、sentinel errors。
- `internal/reducer`：纯 `Reduce(old, event)`。
- 两份 JSON Schema：Event v1、SessionSnapshot v1。
- Schema 语法和 enum 一致性测试。
- 最小 `cmd/taskmaster/main.go`，只需可编译和 `version` 输出。
- README：说明当前只完成 PR 1，不夸大后续能力。

禁止交付：

- Claude/Codex Adapter。
- 文件 Store、锁、原子写入。
- 通知、OpenClaw、CC Switch 修改。
- init/status/watch/doctor 的实际实现。
- daemon、端口、SQLite、Redis、Web UI。

## 5. 不可更改的领域规则

- 持久状态只有 `working / waiting_input / completed / error`。
- `idle` 是没有有效快照的派生状态，不落盘。
- EventKind：`session_started / work_started / progress / input_required / turn_completed / failed / session_ended / cleared`。
- Capability：`full / completion_only / manual`，Event 必填。
- TTL：working 15 分钟；waiting_input 8 小时；completed 2 分钟；error 8 小时。
- Reducer 不得调用 `time.Now()`；全部时间来自 `Event.OccurredAt`。
- Reducer 不得修改传入的旧快照。
- 相同 event ID 幂等，不增加 revision。
- 比旧快照时间早 2 秒以上的事件返回 `ErrStaleEvent`；刚好早 2 秒应接受。
- SessionEnd/Cleared 返回 `Next=nil`，不创建 idle 快照。
- Error fingerprint 使用已经脱敏的 Event.Message 计算 SHA-256。
- 原始 Prompt、响应和工具参数不属于领域模型。

## 6. TDD 执行要求

每个生产行为必须遵守：

1. 先写只验证一个行为的测试。
2. 运行并确认测试因缺少该行为而失败，不是因为语法或环境错误。
3. 写最小实现。
4. 运行目标测试和全量测试，确认通过。
5. 绿灯后才整理命名或去重，再次运行测试。

每个测试名称要说明它防止哪种回归。期望值使用手工字面量，不使用被测代码的 helper 计算。PR 描述中附关键 RED/GREEN 命令与结果摘要。

## 7. 建议提交拆分

1. `feat: define taskmaster domain protocol`
2. `feat: reduce agent lifecycle events`
3. `test: harden reducer ordering and idempotency`
4. `docs: publish reducer contracts and project skeleton`

不要把所有改动压成一个不可审查的大提交。

## 8. 最终验证

必须全部成功：

```bash
gofmt -w cmd internal schemas
go test ./...
go vet ./...
go build ./cmd/taskmaster
git diff --check
git status --short
```

最后删除本地 build 产物。`git status --short` 中只允许本 PR 文件，以及原本就存在且未暂存的 `.DS_Store`。

人工 mutation check：错误 TTL、错误事件映射、删除 duplicate guard、改变 stale 边界、错误 fingerprint 算法，分别必须至少使一个测试失败。

## 9. PR 描述必须包含

- 本 PR 做了什么。
- 明确未做什么。
- Domain 和 Reducer 的公开接口。
- 关键状态转换表。
- RED/GREEN 验证摘要。
- `go test ./...`、`go vet ./...`、`go build ./cmd/taskmaster` 结果。
- 已知限制：尚无 Adapter、Store、通知和安装器。
- 对架构文档的唯一协议修正：Event 必填 `capability`。

## 10. 遇到以下情况立即停下

- 需要改变状态枚举或 TTL。
- 想引入第三方运行时依赖。
- 想提前实现 PR 2 以后功能。
- 无法观察到测试的预期 RED。
- Go 工具链或依赖安装失败。
- 发现文档接口互相矛盾。
- 需要修改 CC Switch 或 Claude 配置。

停止后提交：具体命令、完整错误、已尝试步骤和建议选项，不要猜测性绕过。
