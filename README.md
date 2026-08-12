# TaskMaster

> A zero-bloat, single-binary local state bus and notification system for AI coding agents.

## Scope

**This repository is at PR 1 status only.** The following has been delivered:

- **Domain contract** (`internal/domain`): validated protocol values, event/source/capability kinds, TTL map, snapshot struct, and sentinel errors.
- **Pure Reducer** (`internal/reducer`): `Reduce(old, event)` computes lifecycle transitions without I/O or wall-clock reads.
- **JSON Schemas** (`schemas/`): draft-2020-12 contracts for Event and SessionSnapshot, checked for enum parity with domain code.
- **CLI entrypoint** (`cmd/taskmaster/`): compiles; supports `taskmaster version` only.

The following have **not** been implemented and belong to future PRs:

| PR | Component | Description |
|---|---|---|
| 2 | File Store | Path resolution, locking, atomic writes, revisioning, corruption recovery |
| 3 | Generic Adapter + CLI | `emit --adapter generic`, `status`, `clear`, quiet/strict modes |
| 4 | Notification Policy | `beeep` desktop notifications, circuit breaker, `notify test` |
| 5 | Claude Adapter | P0 Claude Code Hook normalization and fixture tests |
| 6 | Claude + CC Switch Installer | detect/plan/dry-run/backup/commit/verify/uninstall |
| 7 | Watch + Doctor | fsnotify parent-dir watch, full doctor checks |
| 8 | Codex P1 | Full/completion_only/manual probe and TUI/exec reporting |
| 9 | OpenClaw + Release | Safe argv, timeout, circuit breaker, release artifacts |

## Package Map

```
cmd/taskmaster/          # CLI entrypoint (version only in PR 1)
internal/domain/         # Protocol types, validation, TTL, hashes
internal/reducer/        # Pure reducer: (old snapshot, event) → result
schemas/                 # JSON Schema contracts (draft 2020-12)
```

## Build & Test

```bash
go test ./...
go vet ./...
go build ./cmd/taskmaster
```

## Status Protocol (PR 1)

| Status | Meaning | Default TTL |
|---|---|---|
| `working` | Agent is generating or executing tools | 15 min |
| `waiting_input` | Waiting for user input, authorization, or selection | 8 hours |
| `completed` | Current turn finished | 2 min |
| `error` | Agent, auth, quota, network, or tool failure | 8 hours |

`idle` is a derived query state (no valid snapshot exists); it is never persisted.

## Reducer Transition Table (PR 1)

| Event Kind | → Status |
|---|---|
| `session_started` / `work_started` / `progress` | `working` |
| `input_required` | `waiting_input` |
| `turn_completed` | `completed` |
| `failed` | `error` |
| `session_ended` / `cleared` | delete (Next=nil) |

Duplicates (same `event_id` as `last_event_id`) return `IgnoredAsDuplicate=true`, revision unchanged. Events older than `updated_at - 2s` return `ErrStaleEvent`.

## Dependencies

Go standard library only. No third-party packages in PR 1.
