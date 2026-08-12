# PR 1 Domain + Reducer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build TaskMaster's dependency-free domain contract and pure reducer, with executable state-transition tests and JSON Schemas.

**Architecture:** `internal/domain` owns validated protocol values and immutable result types. `internal/reducer` maps `(old snapshot, event)` to a new snapshot or deletion without I/O or wall-clock reads. JSON Schemas mirror those exported contracts and are checked for syntax and enum parity.

**Tech Stack:** Go 1.23+, standard library only, `go test`, JSON Schema draft 2020-12.

## Global Constraints

- Runtime and filesystem names are lowercase `taskmaster`; product display name is `TaskMaster`.
- No daemon, ports, SQLite, Redis, network code, notification code, or Agent adapters in PR 1.
- Production code must be preceded by a test that fails for the expected missing behavior.
- Reducer receives all time through `Event.OccurredAt`; it must never call `time.Now()`.
- Tests use literal expectations and real code; no mocks.
- Raw prompts and tool payloads are outside the domain model.
- Protocol `schema_version` is exactly `1`.
- `Event.capability` is required so the reducer never infers capability from source.

---

## File Map

- Create `go.mod`: module and Go version.
- Create `internal/domain/status.go`: status enum, validity, TTL.
- Create `internal/domain/event.go`: event/source/capability kinds and validation.
- Create `internal/domain/snapshot.go`: snapshot, transition, reduce result, session hash.
- Create `internal/domain/errors.go`: sentinel errors.
- Create `internal/domain/domain_test.go`: protocol validation and TTL tests.
- Create `internal/reducer/reducer.go`: pure reducer.
- Create `internal/reducer/reducer_test.go`: transition, idempotency, stale-event, deletion, revision, and error-fingerprint tests.
- Create `schemas/event.schema.json`: Event v1 contract.
- Create `schemas/session-snapshot.schema.json`: snapshot v1 contract.
- Create `schemas/schema_test.go`: schema syntax and enum parity tests.
- Create `cmd/taskmaster/main.go`: compilable lowercase CLI entrypoint with version output only.
- Create `README.md`: scope, current PR status, package map, test command.

---

### Task 1: Bootstrap module and validated domain types

**Files:**
- Create: `go.mod`
- Create: `internal/domain/domain_test.go`
- Create: `internal/domain/status.go`
- Create: `internal/domain/event.go`
- Create: `internal/domain/snapshot.go`
- Create: `internal/domain/errors.go`

**Interfaces:**
- Produces: `Status.Valid()`, `DefaultTTL(Status)`, `Event.Validate()`, `SessionHash(string)`, and the domain structs used by the reducer.

- [ ] **Step 1: Create `go.mod` and write failing domain tests**

```go
module github.com/taskmaster-dev/taskmaster

go 1.23
```

Tests must include these literal behaviors:

```go
func TestEventValidateRejectsMissingSession(t *testing.T) {
    event := validEvent()
    event.SessionID = ""
    if err := event.Validate(); !errors.Is(err, ErrInvalidInput) {
        t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
    }
}

func TestDefaultTTL(t *testing.T) {
    tests := []struct{ status Status; want time.Duration }{
        {StatusWorking, 15 * time.Minute},
        {StatusWaitingInput, 8 * time.Hour},
        {StatusCompleted, 2 * time.Minute},
        {StatusError, 8 * time.Hour},
    }
    for _, tt := range tests {
        got, err := DefaultTTL(tt.status)
        if err != nil || got != tt.want {
            t.Fatalf("DefaultTTL(%q) = %v, %v; want %v, nil", tt.status, got, err, tt.want)
        }
    }
}

func TestSessionHashUsesSHA256(t *testing.T) {
    const want = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    if got := SessionHash(""); got != want {
        t.Fatalf("SessionHash(empty) = %q, want %q", got, want)
    }
}
```

- [ ] **Step 2: Run RED**

Run: `go test ./internal/domain`

Expected: FAIL because `Event`, `DefaultTTL`, and `SessionHash` do not exist.

- [ ] **Step 3: Implement minimal domain contract**

Use exact exported values:

```go
const SchemaVersion = 1

const (
    StatusWorking Status = "working"
    StatusWaitingInput Status = "waiting_input"
    StatusCompleted Status = "completed"
    StatusError Status = "error"
)

const (
    EventSessionStarted EventKind = "session_started"
    EventWorkStarted EventKind = "work_started"
    EventProgress EventKind = "progress"
    EventInputRequired EventKind = "input_required"
    EventTurnCompleted EventKind = "turn_completed"
    EventFailed EventKind = "failed"
    EventSessionEnded EventKind = "session_ended"
    EventCleared EventKind = "cleared"
)

const (
    SourceHook Source = "hook"
    SourceNotify Source = "notify"
    SourceManual Source = "manual"
    SourceLog Source = "log"
)

const (
    CapabilityFull Capability = "full"
    CapabilityCompletionOnly Capability = "completion_only"
    CapabilityManual Capability = "manual"
)
```

`Event.Validate` must check schema version, safe event ID, safe lowercase agent ID, session ID, enum validity, non-zero UTC-capable timestamp, positive PID when present, metadata count, and Schema max lengths using `utf8.RuneCountInString`.

- [ ] **Step 4: Run GREEN and format**

Run: `gofmt -w internal/domain && go test ./internal/domain`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add go.mod internal/domain
git commit -m "feat: define taskmaster domain protocol"
```

---

### Task 2: Implement lifecycle transitions with a pure reducer

**Files:**
- Create: `internal/reducer/reducer_test.go`
- Create: `internal/reducer/reducer.go`

**Interfaces:**
- Consumes: `domain.Event`, `domain.SessionSnapshot`, `domain.DefaultTTL`.
- Produces: `func Reduce(old *domain.SessionSnapshot, event domain.Event) (domain.ReduceResult, error)`.

- [ ] **Step 1: Write the failing transition table**

```go
func TestReduceMapsLifecycleEvents(t *testing.T) {
    now := time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)
    tests := []struct {
        name string
        old *domain.SessionSnapshot
        kind domain.EventKind
        want *domain.Status
        wantDelete bool
    }{
        {"start creates working", nil, domain.EventSessionStarted, statusPtr(domain.StatusWorking), false},
        {"permission waits", snapshot(domain.StatusWorking, now), domain.EventInputRequired, statusPtr(domain.StatusWaitingInput), false},
        {"progress resumes waiting", snapshot(domain.StatusWaitingInput, now), domain.EventProgress, statusPtr(domain.StatusWorking), false},
        {"stop completes", snapshot(domain.StatusWorking, now), domain.EventTurnCompleted, statusPtr(domain.StatusCompleted), false},
        {"failure errors", snapshot(domain.StatusWorking, now), domain.EventFailed, statusPtr(domain.StatusError), false},
        {"session end deletes", snapshot(domain.StatusWorking, now), domain.EventSessionEnded, nil, true},
        {"clear missing is idempotent", nil, domain.EventCleared, nil, true},
    }
    // Each case calls Reduce with a valid literal event and asserts Next/Transition.
}
```

Also assert revision starts at 1, increments by one, `StartedAt` is preserved, `UpdatedAt` equals event time, and `ExpiresAt` equals event time plus the target TTL.

- [ ] **Step 2: Run RED**

Run: `go test ./internal/reducer -run TestReduceMapsLifecycleEvents -v`

Expected: FAIL because package implementation does not exist.

- [ ] **Step 3: Implement minimal reducer**

Reducer order must be:

1. `event.Validate()`.
2. duplicate event check.
3. stale event check with two-second tolerance.
4. delete-event handling.
5. map event kind to status.
6. create immutable next snapshot and Transition.

Do not mutate `old` and do not call the clock.

- [ ] **Step 4: Run GREEN**

Run: `gofmt -w internal/reducer && go test ./internal/reducer`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/reducer
git commit -m "feat: reduce agent lifecycle events"
```

---

### Task 3: Protect idempotency, stale ordering, and error identity

**Files:**
- Modify: `internal/reducer/reducer_test.go`
- Modify: `internal/reducer/reducer.go`

**Interfaces:**
- Preserves the `Reduce` signature.
- Produces observable `Transition.IgnoredAsDuplicate` and `Transition.ErrorChanged`.

- [ ] **Step 1: Write failing edge-case tests**

```go
func TestReduceIgnoresDuplicateEventID(t *testing.T) {
    old := snapshot(domain.StatusWorking, fixedTime)
    old.LastEventID = "same"
    event := validEvent(domain.EventProgress, fixedTime.Add(time.Second))
    event.EventID = "same"
    got, err := Reduce(old, event)
    if err != nil || !got.Transition.IgnoredAsDuplicate || got.Next.Revision != old.Revision {
        t.Fatalf("duplicate result = %#v, %v", got, err)
    }
}

func TestReduceRejectsEventOlderThanTolerance(t *testing.T) {
    old := snapshot(domain.StatusWorking, fixedTime)
    event := validEvent(domain.EventTurnCompleted, fixedTime.Add(-3*time.Second))
    _, err := Reduce(old, event)
    if !errors.Is(err, domain.ErrStaleEvent) {
        t.Fatalf("error = %v, want ErrStaleEvent", err)
    }
}

func TestReduceMarksChangedErrorFingerprint(t *testing.T) {
    old := snapshot(domain.StatusError, fixedTime)
    old.LastErrorFingerprint = domain.ErrorFingerprint("network timeout")
    event := validEvent(domain.EventFailed, fixedTime.Add(time.Minute))
    event.Message = "quota exceeded"
    got, err := Reduce(old, event)
    if err != nil || !got.Transition.ErrorChanged {
        t.Fatalf("result = %#v, %v; want ErrorChanged", got, err)
    }
}
```

Add the boundary case exactly two seconds old, which must be accepted.

- [ ] **Step 2: Run RED**

Run: `go test ./internal/reducer -run 'Duplicate|Older|Fingerprint' -v`

Expected: at least one FAIL for missing duplicate/stale/fingerprint behavior.

- [ ] **Step 3: Implement minimal edge behavior**

Use SHA-256 of the already-sanitized `Event.Message` for `ErrorFingerprint`. A non-error next state clears the prior error fingerprint. Duplicate returns a value copy of the old snapshot without revision change.

- [ ] **Step 4: Run GREEN and the full suite**

Run: `gofmt -w internal/domain internal/reducer && go test ./...`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain internal/reducer
git commit -m "test: harden reducer ordering and idempotency"
```

---

### Task 4: Publish executable Schemas and repository skeleton

**Files:**
- Create: `schemas/event.schema.json`
- Create: `schemas/session-snapshot.schema.json`
- Create: `schemas/schema_test.go`
- Create: `cmd/taskmaster/main.go`
- Create: `README.md`
- Modify: `TaskMaster.md`

**Interfaces:**
- Produces: two versioned JSON contracts and a compilable `taskmaster` entrypoint.
- Consumes: domain enum string values.

- [ ] **Step 1: Write failing schema contract test**

```go
func TestSchemasAreValidJSONAndMatchDomainEnums(t *testing.T) {
    event := loadSchema(t, "event.schema.json")
    snapshot := loadSchema(t, "session-snapshot.schema.json")

    assertEnum(t, event, "kind", []string{
        "session_started", "work_started", "progress", "input_required",
        "turn_completed", "failed", "session_ended", "cleared",
    })
    assertEnum(t, event, "capability", []string{"full", "completion_only", "manual"})
    assertEnum(t, snapshot, "status", []string{"working", "waiting_input", "completed", "error"})
}
```

The test must use `encoding/json`, resolve files relative to `runtime.Caller`, and assert `capability` is present in each schema's required list.

- [ ] **Step 2: Run RED**

Run: `go test ./schemas -v`

Expected: FAIL because schema files do not exist.

- [ ] **Step 3: Add Schemas and minimal CLI skeleton**

Copy the exact draft-2020-12 contracts from `TaskMaster.md`, adding required `capability` to Event. `cmd/taskmaster/main.go` may support only `taskmaster version` and must print `taskmaster dev`; no future command placeholders.

README must state PR 1 scope, package map, `go test ./...`, and explicitly list future PRs without claiming they exist.

- [ ] **Step 4: Align architecture document**

Add `capability` to Event required/properties in `TaskMaster.md` and state that Adapter supplies it. Do not change other architecture decisions.

- [ ] **Step 5: Run GREEN and quality checks**

Run:

```bash
gofmt -w cmd internal schemas
go test ./...
go vet ./...
go build ./cmd/taskmaster
```

Expected: all commands exit 0 with no warnings. Remove the locally built `taskmaster` artifact before staging.

- [ ] **Step 6: Commit**

```bash
git add README.md TaskMaster.md cmd schemas
git commit -m "docs: publish reducer contracts and project skeleton"
```

---

## Final Verification

- [ ] `gofmt -w cmd internal schemas` produces no subsequent diff.
- [ ] `go test ./...` passes.
- [ ] `go vet ./...` passes.
- [ ] `go build ./cmd/taskmaster` passes.
- [ ] `git diff --check` passes.
- [ ] `git status --short` contains only intentionally committed work and the pre-existing untracked `.DS_Store`.
- [ ] Mutation check: wrong TTL, wrong event mapping, removed duplicate guard, stale boundary change, and wrong error fingerprint each fail at least one test.
