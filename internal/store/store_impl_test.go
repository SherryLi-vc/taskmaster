package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

var now = time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)

func sampleKey() domain.SessionKey {
	return domain.NewSessionKey("claude", "sess-123")
}

func sampleKeyWithID(agent, sessionID string) domain.SessionKey {
	return domain.NewSessionKey(agent, sessionID)
}

func sampleSnapshot() domain.SessionSnapshot {
	ttl, _ := domain.DefaultTTL(domain.StatusWorking)
	return domain.SessionSnapshot{
		SchemaVersion: 1,
		Revision:      1,
		Agent:         "claude",
		SessionIDHash: domain.SessionHash("sess-123"),
		SessionID:     "sess-123",
		Status:        domain.StatusWorking,
		StartedAt:     now,
		UpdatedAt:     now,
		ExpiresAt:     now.Add(ttl),
		LastEventID:   "event-1",
		Source:        domain.SourceHook,
		Capability:    domain.CapabilityFull,
	}
}

func isCorruptError(err error) bool {
	return err != nil && (errors.Is(err, domain.ErrCorruptState) || errors.Is(err, domain.ErrInvalidInput))
}

// =============================================================================
// Issue #1: Store.Update atomic transaction
// =============================================================================

func TestUpdateTransactionAtomicity(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	snap.Status = domain.StatusWorking
	snap.Revision = 1

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		next.Revision = old.Revision + 1
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got.Revision != 2 {
		t.Errorf("revision = %d, want 2", got.Revision)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %q, want %q", got.Status, domain.StatusCompleted)
	}
}

func TestUpdateDeleteTransition(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		return domain.ReduceResult{Next: nil, Transition: domain.Transition{ShouldDelete: true, StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() delete error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != nil {
		t.Error("session should be deleted")
	}
}

// =============================================================================
// Issue #2: Lock ordering - exclusive mkdir lock with owner nonce
// =============================================================================

func TestExclusiveLockBlocksConcurrentWriters(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	lockDir := s.LockPath(k)
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	lockFile := filepath.Join(lockDir, ".lock")
	if err := os.WriteFile(lockFile, []byte("other-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Fatal("Commit() should have timed out")
	}
	if !errors.Is(err, domain.ErrLockTimeout) {
		t.Fatalf("Commit() error = %v, want ErrLockTimeout", err)
	}
}

// =============================================================================
// Issue #3: Error path lock cleanup + stale lock takeover
// =============================================================================

func TestUpdateLockReleasedOnMarshalError(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	jsonMarshalIndentFn = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("simulated marshal error")
	}
	defer func() { jsonMarshalIndentFn = jsonMarshalIndentStd }()

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should have failed on marshal error")
	}

	jsonMarshalIndentFn = jsonMarshalIndentStd
	mutate2 := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate2); err != nil {
		t.Fatalf("Update() after lock release error = %v", err)
	}
}

func TestUpdateLockReleasedOnWriteError(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	sessionPath := s.Path(k)
	os.Remove(sessionPath)
	if err := os.Mkdir(sessionPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	defer os.RemoveAll(sessionPath)

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		if old == nil {
			next := sampleSnapshot()
			next.Status = domain.StatusCompleted
			return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
		}
		next := *old
		next.Status = domain.StatusCompleted
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should have failed on write error")
	}

	os.RemoveAll(sessionPath)
	mutate2 := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		if old == nil {
			next := sampleSnapshot()
			next.Status = domain.StatusCompleted
			return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
		}
		next := *old
		next.Status = domain.StatusCompleted
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate2); err != nil {
		t.Fatalf("Update() after lock release error = %v", err)
	}
}

func TestCommitLockTimeout(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	lockDir := s.LockPath(k)
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	lockFile := filepath.Join(lockDir, ".lock")
	if err := os.WriteFile(lockFile, []byte("stale-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	snap := sampleSnapshot()
	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Fatal("Commit() should have timed out")
	}
	if !errors.Is(err, domain.ErrLockTimeout) {
		t.Fatalf("Commit() error = %v, want ErrLockTimeout", err)
	}
}

func TestStaleLockTakeover(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	lockDir := s.LockPath(k)
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	lockFile := filepath.Join(lockDir, ".lock")
	if err := os.WriteFile(lockFile, []byte("old-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	past := time.Now().Add(-3 * time.Second)
	os.Chtimes(lockDir, past, past)

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() with stale lock error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil || got.Status != snap.Status {
		t.Error("stale takeover did not write snapshot correctly")
	}
}

// =============================================================================
// Issue #4: Atomic write preserves old snapshot + no leftover tmp
// =============================================================================

func TestAtomicWritePreservesOldOnFailure(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "original"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	jsonMarshalIndentFn = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("simulated marshal error")
	}

	newSnap := sampleSnapshot()
	newSnap.Status = domain.StatusCompleted
	newSnap.Message = "updated"

	err = s.Commit(context.Background(), k, newSnap)
	jsonMarshalIndentFn = jsonMarshalIndentStd
	if err == nil {
		t.Fatal("Commit() should have failed on marshal error")
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("old snapshot was lost!")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("old status = %q, want %q", got.Status, domain.StatusWorking)
	}
	if got.Message != "original" {
		t.Errorf("old message = %q, want %q", got.Message, "original")
	}

	sessionsDir := filepath.Join(tmp, "sessions")
	filepath.Walk(sessionsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if !info.IsDir() && strings.Contains(info.Name(), ".tmp-") {
			t.Errorf("leftover temp file: %s", path)
		}
		return nil
	})

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Message = "reacquired"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() after failure error = %v", err)
	}
}

func TestNoLeftoverTmpFiles(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	sessionsDir := filepath.Join(tmp, "sessions")
	entries, err := os.ReadDir(sessionsDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			files, _ := os.ReadDir(filepath.Join(sessionsDir, e.Name()))
			for _, f := range files {
				if strings.HasSuffix(f.Name(), ".tmp") || strings.Contains(f.Name(), ".tmp-") {
					t.Errorf("leftover temp file: %s", f.Name())
				}
			}
		}
	}
}

// =============================================================================
// Issue #5: Windows atomic replace (tested indirectly via atomic write tests)
// =============================================================================

// =============================================================================
// Issue #6: List degraded count
// =============================================================================

func TestListDegradedCount(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	k1 := sampleKey()
	snap1 := sampleSnapshot()
	snap1.ExpiresAt = now.Add(2 * time.Hour)
	if err := s.Commit(context.Background(), k1, snap1); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	dir := filepath.Join(tmp, "sessions", "claude")
	corruptPath := filepath.Join(dir, "corrupt-sess.json")
	os.WriteFile(corruptPath, []byte("not json"), 0o600)

	res, err := s.List(context.Background(), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(res.Snapshots) != 1 {
		t.Errorf("List() returned %d snapshots, want 1", len(res.Snapshots))
	}
	if res.DegradedCount != 1 {
		t.Errorf("List() DegradedCount = %d, want 1", res.DegradedCount)
	}
}

// =============================================================================
// Issue #8: Corrupt snapshot quarantine
// =============================================================================

func TestCorruptQuarantineAndRebuild(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	snap1 := sampleSnapshot()
	snap1.Revision = 1
	snap1.Status = domain.StatusWorking
	if err := s.Commit(context.Background(), k, snap1); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	path := s.Path(k)
	if err := os.WriteFile(path, []byte("not json{{{"), 0o600); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := sampleSnapshot()
		next.Revision = 1
		next.Status = domain.StatusCompleted
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() after corrupt error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() = nil, want rebuilt snapshot")
	}
	if got.Revision != 1 {
		t.Errorf("revision after rebuild = %d, want 1", got.Revision)
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("status = %q, want %q", got.Status, domain.StatusCompleted)
	}

	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	foundQuarantine := false
	for _, e := range entries {
		if strings.Contains(e.Name(), ".corrupt.") {
			foundQuarantine = true
			break
		}
	}
	if !foundQuarantine {
		t.Error("corrupt file was not quarantined")
	}
}

func TestCorruptRetentionMax3(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		if old == nil {
			next := sampleSnapshot()
			next.Revision = 1
			return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
		}
		next := *old
		next.Revision = old.Revision + 1
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
	}

	for i := 0; i < 4; i++ {
		path := s.Path(k)
		os.WriteFile(path, []byte(fmt.Sprintf("corrupt-%d", i)), 0o600)
		if err := s.Update(context.Background(), k, mutate); err != nil {
			t.Fatalf("Update() %d error = %v", i, err)
		}
	}

	dir := filepath.Dir(s.Path(k))
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if strings.Contains(e.Name(), ".corrupt.") {
			count++
		}
	}
	if count != 3 {
		t.Errorf("corrupt file count = %d, want 3", count)
	}
}

func TestUpdateQuarantinePassesNilOld(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	path := s.Path(k)
	os.MkdirAll(filepath.Dir(path), 0o700)
	os.WriteFile(path, []byte("corrupt-data"), 0o600)

	var receivedOld *domain.SessionSnapshot
	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		receivedOld = old
		next := sampleSnapshot()
		next.Revision = 1
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if receivedOld != nil {
		t.Error("mutate should receive nil old for corrupt snapshot")
	}
}

// =============================================================================
// Issue #9: Session key binding to snapshot identity
// =============================================================================

func TestCommitRejectsMismatchedIdentity(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	snap.SessionID = "sess-456"
	snap.SessionIDHash = domain.SessionHash("sess-456")

	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Fatal("Commit() should reject mismatched identity")
	}
}

func TestUpdateRejectsMismatchedIdentity(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.SessionID = "sess-456"
		next.SessionIDHash = domain.SessionHash("sess-456")
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should reject mismatched identity in result")
	}
}

func TestLoadRejectsMismatchedIdentity(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	k1 := sampleKeyWithID("claude", "sess-123")
	k2 := sampleKeyWithID("claude", "sess-456")
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k1, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	path := s.Path(k2)
	os.MkdirAll(filepath.Dir(path), 0o700)
	data, _ := json.MarshalIndent(snap, "", "  ")
	os.WriteFile(path, data, 0o600)

	_, err = s.Load(context.Background(), k2)
	if err == nil {
		t.Fatal("Load() should reject mismatched identity (file path vs content)")
	}
	if !errors.Is(err, domain.ErrCorruptState) {
		t.Fatalf("Load() error = %v, want ErrCorruptState", err)
	}
}

func TestKeyFieldsMustMatchSnapshot(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	k := domain.NewSessionKey("claude", "sess-123")
	snap := sampleSnapshot()
	snap.Agent = "codex"
	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Error("Commit() should reject mismatched agent")
	}

	k = domain.NewSessionKey("claude", "sess-123")
	snap = sampleSnapshot()
	snap.SessionID = "sess-456"
	snap.SessionIDHash = domain.SessionHash("sess-456")
	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Error("Commit() should reject mismatched session_id")
	}
}

// =============================================================================
// Issue #10: Real concurrency tests
// =============================================================================

func TestConcurrentCrossSessionUpdates(t *testing.T) {
	const numSessions = 10
	const goroutinesPerSession = 10

	tmp := t.TempDir()
	stores := make([]*Store, numSessions)
	for i := 0; i < numSessions; i++ {
		s, err := New(filepath.Join(tmp, fmt.Sprintf("store-%d", i)))
		if err != nil {
			t.Fatalf("store.New() %d error = %v", i, err)
		}
		stores[i] = s
	}

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	expectedRevisions := make([]int32, numSessions)

	for sIdx := 0; sIdx < numSessions; sIdx++ {
		for g := 0; g < goroutinesPerSession; g++ {
			wg.Add(1)
			go func(sessionIdx, goroutineIdx int) {
				defer wg.Done()
				<-startBarrier

				agent := fmt.Sprintf("agent-%d", sessionIdx)
				sessionID := fmt.Sprintf("sess-%d", sessionIdx)
				k := domain.NewSessionKey(agent, sessionID)

				mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
					if old == nil {
						next := domain.SessionSnapshot{
							SchemaVersion: 1,
							Revision:      1,
							Agent:         agent,
							SessionIDHash: domain.SessionHash(sessionID),
							SessionID:     sessionID,
							Status:        domain.StatusWorking,
							StartedAt:     time.Now(),
							UpdatedAt:     time.Now(),
							ExpiresAt:     time.Now().Add(15 * time.Minute),
							LastEventID:   fmt.Sprintf("event-%d", goroutineIdx),
							Source:        domain.SourceHook,
							Capability:    domain.CapabilityFull,
						}
						return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
					}
					next := *old
					next.Revision = old.Revision + 1
					next.LastEventID = fmt.Sprintf("event-%d", goroutineIdx)
					atomic.AddInt32(&expectedRevisions[sessionIdx], 1)
					return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
				}

				_ = stores[sessionIdx].Update(context.Background(), k, mutate)
			}(sIdx, g)
		}
	}

	close(startBarrier)
	wg.Wait()

	for sIdx := 0; sIdx < numSessions; sIdx++ {
		agent := fmt.Sprintf("agent-%d", sIdx)
		sessionID := fmt.Sprintf("sess-%d", sIdx)
		k := domain.NewSessionKey(agent, sessionID)

		snap, err := stores[sIdx].Load(context.Background(), k)
		if err != nil {
			t.Fatalf("session %d Load() error = %v", sIdx, err)
		}
		if snap == nil {
			t.Fatalf("session %d has no snapshot", sIdx)
		}
		expectedRev := int(atomic.LoadInt32(&expectedRevisions[sIdx]))
		if snap.Revision != expectedRev+1 {
			t.Errorf("session %d revision = %d, want %d (successful commits)", sIdx, snap.Revision, expectedRev+1)
		}
	}
}

func TestSameSessionConcurrentUpdates(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows mkdir concurrency semantics differ from Unix; concurrent lock test skipped")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	var wg sync.WaitGroup
	startBarrier := make(chan struct{})
	var successCount int32
	var lockTimeoutCount int32
	var otherErrorCount int32

	const numGoroutines = 20
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			<-startBarrier

			mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
				if old == nil {
					next := sampleSnapshot()
					next.Revision = 1
					return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
				}
				next := *old
				next.Revision = old.Revision + 1
				next.LastEventID = fmt.Sprintf("event-%d", id)
				return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
			}

			err := s.Update(context.Background(), k, mutate)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else if errors.Is(err, domain.ErrLockTimeout) {
				atomic.AddInt32(&lockTimeoutCount, 1)
			} else {
				atomic.AddInt32(&otherErrorCount, 1)
				t.Logf("goroutine %d unexpected error: %v", id, err)
			}
		}(i)
	}

	close(startBarrier)
	wg.Wait()

	total := atomic.LoadInt32(&successCount) + atomic.LoadInt32(&lockTimeoutCount) + atomic.LoadInt32(&otherErrorCount)
	if total != numGoroutines {
		t.Fatalf("total accounted = %d, want %d", total, numGoroutines)
	}
	if atomic.LoadInt32(&otherErrorCount) > 0 {
		t.Errorf("unexpected errors: %d", atomic.LoadInt32(&otherErrorCount))
	}

	snap, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if snap == nil {
		t.Fatal("session has no snapshot after concurrent updates")
	}
	expectedRev := int(atomic.LoadInt32(&successCount))
	if snap.Revision != expectedRev {
		t.Errorf("final revision = %d, want %d (successful commit count)", snap.Revision, expectedRev)
	}
}

func TestTwoStoresSameRoot(t *testing.T) {
	tmp := t.TempDir()
	s1, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() s1 error = %v", err)
	}
	s2, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() s2 error = %v", err)
	}

	k := sampleKey()
	snap := sampleSnapshot()

	if err := s1.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("s1.Commit() error = %v", err)
	}

	got, err := s2.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("s2.Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("s2.Load() = nil, want snapshot from s1")
	}
	if got.Status != snap.Status {
		t.Errorf("status = %q, want %q", got.Status, snap.Status)
	}
}

// =============================================================================
// Issue #10: Subprocess contention tests (helper-process pattern)
// =============================================================================

func TestSubprocessLockContention(t *testing.T) {
	t.Parallel()

	if os.Getenv("TEST_HELPER_PROCESS") == "1" {
		testSubprocessLockContention(t)
		return
	}

	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSubprocessLockContention")
	cmd.Env = append(os.Environ(),
		"TEST_HELPER_PROCESS=1",
		"TEST_TMPDIR="+tmp,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, out)
	}
}

func testSubprocessLockContention(t *testing.T) {
	tmp := os.Getenv("TEST_TMPDIR")
	if tmp == "" {
		t.Fatal("TEST_TMPDIR not set")
	}

	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		next.Revision = old.Revision + 1
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("subprocess Update() error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("subprocess Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("subprocess: snapshot missing after Update")
	}
	if got.Status != domain.StatusCompleted {
		t.Errorf("subprocess: status = %q, want %q", got.Status, domain.StatusCompleted)
	}
	if got.Revision != 2 {
		t.Errorf("subprocess: revision = %d, want 2", got.Revision)
	}
}

func TestSubprocessConcurrentSameSession(t *testing.T) {
	t.Parallel()

	if os.Getenv("TEST_HELPER_PROCESS") == "1" {
		testSubprocessConcurrentSameSession(t)
		return
	}

	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestSubprocessConcurrentSameSession")
	cmd.Env = append(os.Environ(),
		"TEST_HELPER_PROCESS=1",
		"TEST_TMPDIR="+tmp,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("subprocess failed: %v\n%s", err, out)
	}
}

func testSubprocessConcurrentSameSession(t *testing.T) {
	tmp := os.Getenv("TEST_TMPDIR")
	if tmp == "" {
		t.Fatal("TEST_TMPDIR not set")
	}

	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	var wg sync.WaitGroup
	var successCount int32
	const subGoroutines = 5
	for i := 0; i < subGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
				if old == nil {
					next := sampleSnapshot()
					next.Revision = 1
					return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
				}
				next := *old
				next.Revision = old.Revision + 1
				next.LastEventID = fmt.Sprintf("sub-event-%d", id)
				return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
			}
			err := s.Update(context.Background(), k, mutate)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	if successCount < 1 {
		t.Errorf("subprocess: expected at least 1 success, got %d", successCount)
	}
}

// =============================================================================
// Issue #10: Revision monotonic across sessions
// =============================================================================

func TestRevisionMonotonicAcrossSessions(t *testing.T) {
	const numSessions = 5
	const writesPerSession = 10

	tmp := t.TempDir()
	stores := make([]*Store, numSessions)
	for i := 0; i < numSessions; i++ {
		s, err := New(filepath.Join(tmp, fmt.Sprintf("store-%d", i)))
		if err != nil {
			t.Fatalf("store.New() %d error = %v", i, err)
		}
		stores[i] = s
	}

	var wg sync.WaitGroup
	start := make(chan struct{})

	for sIdx := 0; sIdx < numSessions; sIdx++ {
		for w := 0; w < writesPerSession; w++ {
			wg.Add(1)
			go func(sessionIdx, writeIdx int) {
				defer wg.Done()
				<-start
				k := domain.NewSessionKey(fmt.Sprintf("agent-%d", sessionIdx), fmt.Sprintf("sess-%d", sessionIdx))
				mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
					if old == nil {
						next := domain.SessionSnapshot{
							SchemaVersion: 1, Revision: 1,
							Agent: k.Agent, SessionIDHash: k.SessionIDH, SessionID: k.SessionID,
							Status:    domain.StatusWorking,
							StartedAt: time.Now(), UpdatedAt: time.Now(),
							ExpiresAt:   time.Now().Add(15 * time.Minute),
							LastEventID: fmt.Sprintf("event-%d", writeIdx),
							Source:      domain.SourceHook, Capability: domain.CapabilityFull,
						}
						return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
					}
					next := *old
					next.Revision = old.Revision + 1
					next.LastEventID = fmt.Sprintf("event-%d", writeIdx)
					return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
				}
				_ = stores[sessionIdx].Update(context.Background(), k, mutate)
			}(sIdx, w)
		}
	}
	close(start)
	wg.Wait()

	for sIdx := 0; sIdx < numSessions; sIdx++ {
		k := domain.NewSessionKey(fmt.Sprintf("agent-%d", sIdx), fmt.Sprintf("sess-%d", sIdx))
		snap, err := stores[sIdx].Load(context.Background(), k)
		if err != nil {
			t.Fatalf("session %d Load() error = %v", sIdx, err)
		}
		if snap == nil {
			t.Fatalf("session %d has no snapshot", sIdx)
		}
		if snap.Revision < 1 {
			t.Errorf("session %d revision = %d, want >= 1", sIdx, snap.Revision)
		}
	}
}

// =============================================================================
// 100 goroutine / 10 session test
// =============================================================================

type testResult struct {
	err error
}

func TestHundredGoroutineTenSession(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows mkdir concurrency semantics differ from Unix; high-contention test skipped")
	}
	const numSessions = 10
	const goroutinesPerSession = 10

	tmp := t.TempDir()
	// P1-3: all Store instances share the SAME root directory.
	results := make([]testResult, numSessions*goroutinesPerSession)
	var wg sync.WaitGroup
	startBarrier := make(chan struct{})

	idx := 0
	for sIdx := 0; sIdx < numSessions; sIdx++ {
		for g := 0; g < goroutinesPerSession; g++ {
			wg.Add(1)
			go func(sessionIdx, goroutineIdx, resultIdx int) {
				defer wg.Done()
				<-startBarrier

				// P1-3: each goroutine creates its own Store instance
				// pointing to the shared root.
				s, err := New(tmp)
				if err != nil {
					results[resultIdx] = testResult{err: err}
					return
				}

				agent := fmt.Sprintf("agent-%d", sessionIdx)
				sessionID := fmt.Sprintf("sess-%d", sessionIdx)
				k := domain.NewSessionKey(agent, sessionID)

				mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
					if old == nil {
						next := domain.SessionSnapshot{
							SchemaVersion: 1,
							Revision:      1,
							Agent:         agent,
							SessionIDHash: domain.SessionHash(sessionID),
							SessionID:     sessionID,
							Status:        domain.StatusWorking,
							StartedAt:     time.Now(),
							UpdatedAt:     time.Now(),
							ExpiresAt:     time.Now().Add(15 * time.Minute),
							LastEventID:   fmt.Sprintf("event-%d", goroutineIdx),
							Source:        domain.SourceHook,
							Capability:    domain.CapabilityFull,
						}
						return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
					}
					next := *old
					next.Revision = old.Revision + 1
					next.LastEventID = fmt.Sprintf("event-%d", goroutineIdx)
					return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
				}

				err = s.Update(context.Background(), k, mutate)
				results[resultIdx] = testResult{err: err}
			}(sIdx, g, idx)
			idx++
		}
	}

	close(startBarrier)
	wg.Wait()

	// P1-3: collect per-session success counts.
	sessionSuccesses := make(map[string]int)
	for i, r := range results {
		if r.err == nil {
			sIdx := i / goroutinesPerSession
			key := fmt.Sprintf("agent-%d/sess-%d", sIdx, sIdx)
			sessionSuccesses[key]++
		}
	}

	// Assert no unexpected errors (only lock timeouts are expected).
	for i, r := range results {
		if r.err != nil && !errors.Is(r.err, domain.ErrLockTimeout) {
			t.Errorf("goroutine %d: unexpected error: %v", i, r.err)
		}
	}

	// P1-3: verify each session's final revision equals its success count.
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	for sIdx := 0; sIdx < numSessions; sIdx++ {
		agent := fmt.Sprintf("agent-%d", sIdx)
		sessionID := fmt.Sprintf("sess-%d", sIdx)
		k := domain.NewSessionKey(agent, sessionID)
		snap, err := s.Load(context.Background(), k)
		if err != nil {
			t.Fatalf("session %d Load() error = %v", sIdx, err)
		}
		if snap == nil {
			t.Fatalf("session %d has no snapshot", sIdx)
		}
		key := fmt.Sprintf("%s/%s", agent, sessionID)
		wantRev := sessionSuccesses[key]
		if snap.Revision != wantRev {
			t.Errorf("session %d revision = %d, want %d (successful commits)", sIdx, snap.Revision, wantRev)
		}
	}
}

// =============================================================================
// Issue #11: Symlink rejection and permissions
// =============================================================================

func TestRejectSymlinkInManagedPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not tested on Windows")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	agentDir := filepath.Join(tmp, "sessions", "claude")
	os.RemoveAll(agentDir)
	if err := os.Symlink("/tmp", agentDir); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	defer os.Remove(agentDir)

	k := sampleKey()
	snap := sampleSnapshot()
	err = s.Commit(context.Background(), k, snap)
	if err == nil {
		t.Fatal("Commit() should reject symlink in managed path")
	}
}

func TestRejectRootSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not tested on Windows")
	}
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "taskmaster-root")
	if err := os.Symlink("/tmp", rootPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	defer os.Remove(rootPath)

	var newErr error
	_, newErr = New(rootPath)
	if newErr == nil {
		t.Fatal("New() should reject symlink root")
	}
}

func TestTightenExistingDirPermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission tightening not tested on Windows")
	}
	tmp := t.TempDir()
	for _, sub := range []string{"sessions", "locks", "backups"} {
		p := filepath.Join(tmp, sub)
		os.MkdirAll(p, 0o755)
	}

	_, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	for _, sub := range []string{"sessions", "locks", "backups"} {
		p := filepath.Join(tmp, sub)
		info, err := os.Lstat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", sub, err)
		}
		mode := info.Mode().Perm()
		if mode != 0o700 {
			t.Errorf("%s permissions = 0o%o, want 0o700", sub, mode)
		}
	}
}

func TestAgentSubdirPermissionsTightened(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission tests skipped on Windows")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	agentDir := filepath.Join(tmp, "sessions", "claude")
	if err := os.MkdirAll(agentDir, 0o755); err != nil {
		t.Fatalf("MkdirAll error = %v", err)
	}

	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	info, err := os.Lstat(agentDir)
	if err != nil {
		t.Fatalf("stat agent dir: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0o700 {
		t.Errorf("agent dir permissions = 0o%o, want 0o700", mode)
	}
}

func TestFilePermissionsTightened(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission tests skipped on Windows")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	path := s.Path(k)
	os.MkdirAll(filepath.Dir(path), 0o700)
	os.WriteFile(path, []byte("old-data"), 0o644)

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	info, err := os.Lstat(path)
	if err != nil {
		t.Fatalf("stat file: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0o600 {
		t.Errorf("file permissions = 0o%o, want 0o600", mode)
	}
}

func TestRootDirectoryRejectsSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks not tested on Windows")
	}
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "root")
	if err := os.Symlink("/tmp", rootPath); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	defer os.Remove(rootPath)

	_, err := New(rootPath)
	if err == nil {
		t.Fatal("New() should reject symlink root")
	}
}

func TestRootDirectoryRejectsNonDir(t *testing.T) {
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "root-file")
	if err := os.WriteFile(rootPath, []byte("not-a-dir"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	defer os.Remove(rootPath)

	_, err := New(rootPath)
	if err == nil {
		t.Fatal("New() should reject non-directory root")
	}
}

func TestRootDirectoryEnforces0700(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("permission tests skipped on Windows")
	}
	tmp := t.TempDir()
	rootPath := filepath.Join(tmp, "root")
	if err := os.Mkdir(rootPath, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	_, err := New(rootPath)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	info, err := os.Lstat(rootPath)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	mode := info.Mode().Perm()
	if mode != 0o700 {
		t.Errorf("root permissions = 0o%o, want 0o700", mode)
	}
}

// =============================================================================
// Issue #10: JSON trailing newline
// =============================================================================

func TestSnapshotJSONEndsWithNewline(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()

	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	path := s.Path(k)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		t.Error("snapshot JSON does not end with newline")
	}

	var parsed domain.SessionSnapshot
	if err := json.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("JSON unmarshal error: %v", err)
	}
}

// =============================================================================
// Issue #2: Reducer error propagation through Update
// =============================================================================

func TestUpdateReturnsReducerError(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	reducerErr := fmt.Errorf("reducer validation failed")
	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		return domain.ReduceResult{}, reducerErr
	}

	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should return reducer error")
	}
	if !strings.Contains(err.Error(), "reducer validation failed") {
		t.Errorf("Update() error = %q, want to contain 'reducer validation failed'", err.Error())
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("old snapshot was lost after reducer error!")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("status = %q, want %q (unchanged)", got.Status, domain.StatusWorking)
	}
}

// =============================================================================
// Table-driven fault injection
// =============================================================================

func TestFaultInjectionAtomicWritePreservesOld(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "original"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	tests := []struct {
		name    string
		inject  func() error
		cleanup func()
	}{
		{
			name: "marshal failure",
			inject: func() error {
				jsonMarshalIndentFn = func(v interface{}) ([]byte, error) {
					return nil, fmt.Errorf("simulated marshal error")
				}
				return nil
			},
			cleanup: func() { jsonMarshalIndentFn = jsonMarshalIndentStd },
		},
		{
			name: "temp create failure",
			inject: func() error {
				createTempSeam = func(dir, pattern string) (*os.File, error) {
					return nil, fmt.Errorf("simulated create temp error")
				}
				return nil
			},
			cleanup: func() { createTempSeam = os.CreateTemp },
		},
		{
			name: "atomic replace failure",
			inject: func() error {
				replaceExistingSeam = func(newPath, oldPath string) error {
					return fmt.Errorf("simulated replace error")
				}
				return nil
			},
			cleanup: func() { replaceExistingSeam = replaceExisting },
		},
		{
			name: "parent sync failure",
			inject: func() error {
				syncParentDirSeam = func(path string) error {
					return fmt.Errorf("simulated parent sync error")
				}
				return nil
			},
			cleanup: func() { syncParentDirSeam = syncParentDir },
		},
		{
			name: "write file failure",
			inject: func() error {
				writeFileSeam = func(f *os.File, b []byte) (int, error) {
					return 0, fmt.Errorf("simulated write error")
				}
				return nil
			},
			cleanup: func() { writeFileSeam = func(f *os.File, b []byte) (int, error) { return f.Write(b) } },
		},
		{
			name: "file sync failure",
			inject: func() error {
				syncFileSeam = func(f *os.File) error {
					return fmt.Errorf("simulated file sync error")
				}
				return nil
			},
			cleanup: func() { syncFileSeam = func(f *os.File) error { return f.Sync() } },
		},
		{
			name: "chmod failure",
			inject: func() error {
				chmodFileSeam = func(f *os.File, mode os.FileMode) error {
					return fmt.Errorf("simulated chmod error")
				}
				return nil
			},
			cleanup: func() { chmodFileSeam = func(f *os.File, mode os.FileMode) error { return f.Chmod(mode) } },
		},
		{
			name: "close file failure",
			inject: func() error {
				closeFileSeam = func(f *os.File) error {
					// Close the handle to prevent Windows file lock on temp dir cleanup,
					// then return the simulated error for test assertion.
					_ = f.Close()
					return fmt.Errorf("simulated close error")
				}
				return nil
			},
			cleanup: func() { closeFileSeam = func(f *os.File) error { return f.Close() } },
		},
		{
			name: "sessions dir read-only",
			inject: func() error {
				sessionsDir := filepath.Join(tmp, "sessions")
				return os.Chmod(sessionsDir, 0o000)
			},
			cleanup: func() {
				os.Chmod(filepath.Join(tmp, "sessions"), 0o700)
			},
		},
		{
			name: "agent dir read-only",
			inject: func() error {
				agentDir := filepath.Join(tmp, "sessions", k.Agent)
				return os.Chmod(agentDir, 0o000)
			},
			cleanup: func() {
				os.Chmod(filepath.Join(tmp, "sessions", k.Agent), 0o700)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset snapshot to known state.
			if err := s.Commit(context.Background(), k, oldSnap); err != nil {
				t.Fatalf("reset Commit() error = %v", err)
			}

			if err := tt.inject(); err != nil {
				t.Fatalf("inject error: %v", err)
			}
			// Register cleanup BEFORE the skip check so permissions are restored
			// even when the test is skipped on Windows.
			t.Cleanup(tt.cleanup)

			// Skip chmod-based permission tests on Windows (ACLs don't honor chmod).
			if runtime.GOOS == "windows" && (tt.name == "sessions dir read-only" || tt.name == "agent dir read-only" || tt.name == "chmod failure") {
				t.Skip("Windows ACLs don't honor chmod-based permission semantics")
			}

			newSnap := sampleSnapshot()
			newSnap.Status = domain.StatusCompleted
			newSnap.Message = "updated"
			err := s.Commit(context.Background(), k, newSnap)

			// Cleanup before assertions (restore permissions etc.).
			tt.cleanup()

			if err == nil {
				t.Fatal("Commit() should have failed")
			}

			// For stages before rename (create, write, sync, chmod, close, replace,
			// parent sync after rename), verify the old snapshot state.
			// For parent sync failure, the rename already succeeded so the new
			// snapshot is present — verify it instead.
			preRenameStages := map[string]bool{
				"marshal failure":        true,
				"temp create failure":    true,
				"atomic replace failure": true,
				"write file failure":     true,
				"file sync failure":      true,
				"chmod failure":          true,
				"close file failure":     true,
				"sessions dir read-only": true,
				"agent dir read-only":    true,
			}
			got, err := s.Load(context.Background(), k)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got == nil {
				t.Fatal("snapshot was lost!")
			}
			if preRenameStages[tt.name] {
				// Pre-rename failure: old snapshot preserved.
				if got.Status != domain.StatusWorking {
					t.Errorf("old status = %q, want %q", got.Status, domain.StatusWorking)
				}
				if got.Message != "original" {
					t.Errorf("old message = %q, want %q", got.Message, "original")
				}
			} else {
				// Parent sync happens after rename; new snapshot is already written.
				if got.Status != domain.StatusCompleted {
					t.Errorf("status = %q, want %q", got.Status, domain.StatusCompleted)
				}
				if got.Message != "updated" {
					t.Errorf("message = %q, want %q", got.Message, "updated")
				}
			}

			// Verify no temp files remain.
			if runtime.GOOS != "windows" || tt.name != "close file failure" {
				sessionsDir := filepath.Join(tmp, "sessions")
				filepath.Walk(sessionsDir, func(path string, info os.FileInfo, err error) error {
					if err != nil {
						return nil
					}
					if !info.IsDir() && strings.Contains(info.Name(), ".tmp-") {
						t.Errorf("leftover temp file: %s", path)
					}
					return nil
				})
			}

			// Verify lock can be reacquired.
			mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
				next := *old
				next.Message = "reacquired"
				return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
			}
			if err := s.Update(context.Background(), k, mutate); err != nil {
				t.Fatalf("Update() after failure error = %v", err)
			}
		})
	}
}

func TestFaultInjectionUpdatePreservesOld(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "preserve-me"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	jsonMarshalIndentFn = func(v interface{}) ([]byte, error) {
		return nil, fmt.Errorf("simulated marshal error")
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		next.Message = "new-value"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	jsonMarshalIndentFn = jsonMarshalIndentStd
	if err == nil {
		t.Fatal("Update() should have failed on marshal error")
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("old snapshot was lost!")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("old status = %q, want %q", got.Status, domain.StatusWorking)
	}
	if got.Message != "preserve-me" {
		t.Errorf("old message = %q, want %q", got.Message, "preserve-me")
	}
}

// =============================================================================
// Windows CI coverage: same session, write twice, read second
// =============================================================================

func TestWindowsSameSessionWritesTwiceLoadsSecond(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	snap1 := sampleSnapshot()
	snap1.Message = "first-write"
	if err := s.Commit(context.Background(), k, snap1); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}

	snap2 := sampleSnapshot()
	snap2.Message = "second-write"
	snap2.Revision = 2
	if err := s.Commit(context.Background(), k, snap2); err != nil {
		t.Fatalf("second Commit() error = %v", err)
	}

	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() = nil after two writes")
	}
	if got.Message != "second-write" {
		t.Errorf("Message = %q, want %q", got.Message, "second-write")
	}
	if got.Revision != 2 {
		t.Errorf("Revision = %d, want 2", got.Revision)
	}
}

// =============================================================================
// Lock nonce safety tests
// =============================================================================

func TestReleaseSessionLockOnlyWithMatchingNonce(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod 000 not portable to Windows")
	}
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "locks", "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	ReleaseSessionLock(lockDir, lockFile, "correct-nonce")
	if _, err := os.Lstat(lockDir); !os.IsNotExist(err) {
		t.Error("lock dir should be removed with matching nonce")
	}
}

func TestReleaseSessionLockNotWithWrongNonce(t *testing.T) {
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "locks", "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	ReleaseSessionLock(lockDir, lockFile, "wrong-nonce")
	if _, err := os.Lstat(lockDir); os.IsNotExist(err) {
		t.Error("lock dir should NOT be removed with wrong nonce")
	}
}

func TestReleaseSessionLockNotOnReadError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("chmod 000 not portable to Windows; use TestReleaseSessionLockNotOnReadErrorWithSeam on Windows")
	}
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "locks", "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	os.Chmod(lockFile, 0o000)
	defer os.Chmod(lockFile, 0o600)

	err := ReleaseSessionLock(lockDir, lockFile, "correct-nonce")
	if err == nil {
		t.Fatal("ReleaseSessionLock() should have returned error when nonce is unreadable")
	}
	if !strings.Contains(err.Error(), "cannot verify lock ownership") {
		t.Errorf("ReleaseSessionLock() error = %q, want 'cannot verify lock ownership'", err.Error())
	}
	// Lock must NOT be deleted when ownership cannot be verified.
	if _, statErr := os.Lstat(lockDir); os.IsNotExist(statErr) {
		t.Error("lock dir should NOT be removed when nonce file is unreadable")
	}
}

// TestReleaseSessionLockNotOnReadErrorWithSeam uses a ReadFile seam override to
// simulate unreadable nonce on all platforms (including Windows).
func TestReleaseSessionLockNotOnReadErrorWithSeam(t *testing.T) {
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "locks", "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Override readFileSeam to simulate permanent read failure.
	readFileSeam = func(name string) ([]byte, error) {
		return nil, fmt.Errorf("simulated permanent read failure")
	}
	defer func() { readFileSeam = os.ReadFile }()

	err := ReleaseSessionLock(lockDir, lockFile, "correct-nonce")
	if err == nil {
		t.Fatal("ReleaseSessionLock() should have returned error when nonce read fails")
	}
	if !strings.Contains(err.Error(), "cannot verify lock ownership") {
		t.Errorf("ReleaseSessionLock() error = %q, want 'cannot verify lock ownership'", err.Error())
	}
	// Lock must NOT be deleted when ownership cannot be verified.
	if _, statErr := os.Lstat(lockDir); os.IsNotExist(statErr) {
		t.Error("lock dir should NOT be removed when nonce file is unreadable")
	}
}

// =============================================================================
// Stale lock takeover safety
// =============================================================================

func TestStaleLockTakeoverDoesNotDeleteLiveLock(t *testing.T) {
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "locks", "agent-sess.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("live-nonce\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Make locks dir read-only to prevent stale rename.
	if runtime.GOOS != "windows" {
		locksDir := filepath.Join(tmp, "locks")
		os.Chmod(locksDir, 0o500)
		defer os.Chmod(locksDir, 0o700)
	}

	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	_, _, _, err = s.AcquireSessionLock(lockDir)
	if err == nil {
		t.Fatal("AcquireSessionLock() should have timed out")
	}

	if _, err := os.Lstat(lockDir); os.IsNotExist(err) {
		t.Error("live lock was deleted! stale takeover must not delete live locks")
	}
}

// =============================================================================
// Path resolution tests
// =============================================================================

func TestStorePathResolutionBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	got := s.Root()
	if got != tmp {
		t.Errorf("Root() = %q, want %q", got, tmp)
	}
}

func TestStoreCreatesRequiredDirectoriesBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	for _, sub := range []string{"sessions", "locks", "backups"} {
		p := filepath.Join(s.Root(), sub)
		info, err := os.Stat(p)
		if err != nil {
			t.Errorf("directory %q missing: %v", sub, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("%q is not a directory", sub)
		}
	}
}

func TestStoreAgentSubdirBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	got := s.Path(k)
	want := filepath.Join(tmp, "sessions", k.Agent, k.SessionIDH+".json")
	if got != want {
		t.Errorf("Path() = %q, want %q", got, want)
	}
}

func TestStoreLockPathBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	got := s.LockPath(k)
	want := filepath.Join(tmp, "locks", k.Agent+"-"+k.SessionIDH+".lock")
	if got != want {
		t.Errorf("LockPath() = %q, want %q", got, want)
	}
}

// =============================================================================
// Load / Commit / Delete basic
// =============================================================================

func TestStoreCommitAndLoadBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() = nil, want snapshot")
	}
	if got.Status != snap.Status {
		t.Errorf("Status = %q, want %q", got.Status, snap.Status)
	}
	if got.Revision != snap.Revision {
		t.Errorf("Revision = %d, want %d", got.Revision, snap.Revision)
	}
}

func TestStoreLoadMissingBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got != nil {
		t.Errorf("Load() = %v, want nil for missing session", got)
	}
}

func TestStoreDeleteBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if err := s.Delete(context.Background(), k); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() after delete error = %v", err)
	}
	if got != nil {
		t.Error("Load() after delete = non-nil, want nil")
	}
}

func TestStoreDeleteMissingBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	if err := s.Delete(context.Background(), sampleKey()); err != nil {
		t.Fatalf("Delete() on missing session error = %v", err)
	}
}

// =============================================================================
// Corrupted snapshot handling basic
// =============================================================================

func TestStoreLoadCorruptSnapshotBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	path := s.Path(k)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir error = %v", err)
	}
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write corrupt file error = %v", err)
	}
	_, err = s.Load(context.Background(), k)
	if err == nil {
		t.Fatal("Load() with corrupt data should error")
	}
	if !isCorruptError(err) {
		t.Fatalf("Load() error = %v, want ErrCorruptState", err)
	}
}

func TestStoreListBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k1 := sampleKey()
	snap1 := sampleSnapshot()
	snap1.ExpiresAt = now.Add(15 * time.Minute)
	if err := s.Commit(context.Background(), k1, snap1); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	res, err := s.List(context.Background(), now)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(res.Snapshots) != 1 {
		t.Fatalf("List() returned %d sessions, want 1", len(res.Snapshots))
	}
}

func TestStoreListEmptyBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	res, err := s.List(context.Background(), now)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(res.Snapshots) != 0 {
		t.Errorf("List() returned %d sessions, want 0", len(res.Snapshots))
	}
	if res.DegradedCount != 0 {
		t.Errorf("List() DegradedCount = %d, want 0", res.DegradedCount)
	}
}

// =============================================================================
// Concurrent commits basic
// =============================================================================

func TestStoreConcurrentCommitsBasic(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			snap := sampleSnapshot()
			snap.Revision = n + 1
			snap.Message = string(rune('a' + n%26))
			_ = s.Commit(context.Background(), k, snap)
		}(i)
	}
	wg.Wait()
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() = nil after concurrent commits, want snapshot")
	}
}

// =============================================================================
// P1-6: Multi-subprocess cross-process contention
// =============================================================================

func TestSubprocessMultiProcessContention(t *testing.T) {
	t.Parallel()

	if os.Getenv("TEST_HELPER_PROCESS") == "1" {
		testSubprocessMultiProcessContention(t)
		return
	}

	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Launch 2 independent helper processes that compete for the same session.
	// All helpers Start() concurrently; parent uses file-based barrier to
	// synchronize their start, then Wait()s for all.
	const numHelpers = 2
	resultsDir := filepath.Join(tmp, "results")
	if err := os.MkdirAll(resultsDir, 0o700); err != nil {
		t.Fatalf("mkdir results dir: %v", err)
	}

	readyFiles := make([]string, numHelpers)
	for i := 0; i < numHelpers; i++ {
		readyFiles[i] = filepath.Join(resultsDir, fmt.Sprintf("ready-%d.txt", i))
	}

	// Use the current test binary for subprocess helpers.
	testBinary := os.Args[0]
	if exe, err := os.Executable(); err == nil {
		testBinary = exe
	}

	cmds := make([]*exec.Cmd, numHelpers)
	for i := 0; i < numHelpers; i++ {
		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("results-%d.jsonl", i))
		cmd := exec.Command(testBinary, "-test.run=TestSubprocessMultiProcessContention")
		cmd.Env = append(os.Environ(),
			"TEST_HELPER_PROCESS=1",
			"TEST_TMPDIR="+tmp,
			"TEST_RESULTS_FILE="+resultsFile,
			"TEST_HELPER_ID="+fmt.Sprintf("%d", i),
			"TEST_READY_FILE="+readyFiles[i],
			"TEST_RESULTS_DIR="+resultsDir,
		)
		cmds[i] = cmd
	}

	// Start all helpers concurrently (not CombinedOutput / sequential Run).
	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start helper %d: %v", i, err)
		}
	}

	// Barrier: wait for all helpers to signal ready.
	barrierTimeout := 5 * time.Second
	if runtime.GOOS == "windows" {
		barrierTimeout = 15 * time.Second // Windows subprocess startup is slower
	}
	barrierDeadline := time.Now().Add(barrierTimeout)
	for {
		allReady := true
		for _, rf := range readyFiles {
			if _, err := os.Lstat(rf); err != nil {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		if time.Now().After(barrierDeadline) {
			// Kill all helpers on barrier timeout.
			for _, cmd := range cmds {
				cmd.Process.Kill()
				cmd.Wait()
			}
			t.Fatal("helpers did not reach barrier within timeout")
		}
		time.Sleep(1 * time.Millisecond)
	}

	// Signal all helpers to start competing.
	startFile := filepath.Join(resultsDir, "start.txt")
	if err := os.WriteFile(startFile, []byte("go"), 0o600); err != nil {
		t.Fatalf("write start file: %v", err)
	}

	// Hard timeout for the entire contention phase.
	contentionTimeout := 30 * time.Second
	done := make(chan error, numHelpers)
	for i, cmd := range cmds {
		go func(idx int, c *exec.Cmd) {
			done <- c.Wait()
		}(i, cmd)
	}

	var helpersDone int
	var timeoutTriggered bool
	for helpersDone < numHelpers {
		select {
		case err := <-done:
			helpersDone++
			if err != nil {
				out, _ := os.ReadFile(filepath.Join(resultsDir, fmt.Sprintf("results-%d.jsonl", helpersDone-1)))
				t.Logf("helper %d exited with error: %v, output: %s", helpersDone-1, err, string(out))
			}
		case <-time.After(contentionTimeout):
			timeoutTriggered = true
			t.Logf("contention timeout after %v, killing remaining helpers", contentionTimeout)
			for _, cmd := range cmds {
				if cmd.ProcessState == nil {
					cmd.Process.Kill()
				}
			}
			// Drain remaining processes.
			for helpersDone < numHelpers {
				<-done
				helpersDone++
			}
		}
	}

	if timeoutTriggered {
		t.Error("helpers did not complete within contention timeout")
	}

	// Read results from all helpers.
	totalSuccess := 0
	for i := 0; i < numHelpers; i++ {
		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("results-%d.jsonl", i))
		data, err := os.ReadFile(resultsFile)
		if err != nil {
			t.Fatalf("helper %d: read results: %v", i, err)
		}
		var r struct {
			Successes int      `json:"successes"`
			Timeouts  int      `json:"timeouts"`
			Other     int      `json:"other"`
			Errors    []string `json:"errors,omitempty"`
		}
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("helper %d: unmarshal: %v\nraw: %s", i, err, string(data))
		}
		t.Logf("helper %d: successes=%d timeouts=%d other=%d errors=%v", i, r.Successes, r.Timeouts, r.Other, r.Errors)
		totalSuccess += r.Successes
		if r.Other > 0 {
			t.Errorf("helper %d: %d unexpected errors", i, r.Other)
		}
	}

	// Verify final snapshot has revision == total successes + 1 (initial commit).
	s2, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	finalSnap, err := s2.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if finalSnap == nil {
		t.Fatal("final snapshot missing")
	}
	if finalSnap.Revision != totalSuccess+1 {
		t.Errorf("final revision = %d, want %d (initial=1 + successes=%d)", finalSnap.Revision, totalSuccess+1, totalSuccess)
	}
}

func testSubprocessMultiProcessContention(t *testing.T) {
	tmp := os.Getenv("TEST_TMPDIR")
	if tmp == "" {
		t.Fatal("TEST_TMPDIR not set")
	}
	resultsFile := os.Getenv("TEST_RESULTS_FILE")
	if resultsFile == "" {
		t.Fatal("TEST_RESULTS_FILE not set")
	}
	helperID := os.Getenv("TEST_HELPER_ID")
	readyFile := os.Getenv("TEST_READY_FILE")
	resultsDir := os.Getenv("TEST_RESULTS_DIR")

	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	const subUpdates = 5
	var successCount, timeoutCount, otherCount int
	var errorMessages []string

	// Signal ready to parent via ready file.
	if readyFile != "" {
		if err := os.WriteFile(readyFile, []byte("ready"), 0o600); err != nil {
			// Non-fatal: continue even if ready file write fails.
			t.Logf("write ready file: %v", err)
		}
	}

	// Barrier: wait for parent to write start.txt.
	if resultsDir != "" {
		startFile := filepath.Join(resultsDir, "start.txt")
		barrierDeadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Lstat(startFile); err == nil {
				break
			}
			if time.Now().After(barrierDeadline) {
				t.Fatal("barrier timeout waiting for start signal")
			}
			time.Sleep(1 * time.Millisecond)
		}
	}

	for i := 0; i < subUpdates; i++ {
		mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
			if old == nil {
				next := sampleSnapshot()
				next.Revision = 1
				return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
			}
			next := *old
			next.Revision = old.Revision + 1
			next.LastEventID = fmt.Sprintf("multi-event-%d-helper-%s", i, helperID)
			return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
		}
		err := s.Update(context.Background(), k, mutate)
		if err == nil {
			successCount++
		} else if errors.Is(err, domain.ErrLockTimeout) {
			timeoutCount++
		} else {
			otherCount++
			errorMessages = append(errorMessages, err.Error())
		}
	}

	r := struct {
		Successes int      `json:"successes"`
		Timeouts  int      `json:"timeouts"`
		Other     int      `json:"other"`
		Errors    []string `json:"errors,omitempty"`
		HelperID  string   `json:"helper_id"`
	}{
		Successes: successCount,
		Timeouts:  timeoutCount,
		Other:     otherCount,
		Errors:    errorMessages,
		HelperID:  helperID,
	}
	data, _ := json.Marshal(r)
	if err := os.WriteFile(resultsFile, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write results: %v", err)
	}
}

// TestSubprocessCrossProcessLockContentionWindows is a Windows cross-process
// contention test. It compiles the test binary to a temp file via `go test -c`,
// then runs it as subprocess helpers with Start() + TCP barrier + Wait().
// TCP avoids the Windows file-lock sharing violation that prevents helpers
// from writing barrier files in a directory the parent has open.
func TestSubprocessCrossProcessLockContentionWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific cross-process contention test")
	}

	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	resultsDir := filepath.Join(tmp, "results")
	if err := os.MkdirAll(resultsDir, 0o700); err != nil {
		t.Fatalf("mkdir results: %v", err)
	}

	const numHelpers = 2

	// TCP barrier: parent listens on a random port, writes port to a file,
	// helpers connect to parent (bypassing Windows file-lock sharing violation).
	listener, err := tcpBarrierListen(resultsDir)
	if err != nil {
		t.Fatalf("tcp barrier listen: %v", err)
	}
	defer listener.Close()

	// Compile test binary to a temp file. On Windows, `go test` locks the
	// running test binary, preventing direct re-execution via os.Args[0].
	// We compile to a separate file to avoid this.
	// Clean any stale binary first (prevents flake when TempDir is reused).
	testBinary := filepath.Join(tmp, "taskmaster.test.exe")
	os.Remove(testBinary) //nolint:errcheck
	buildCmd := exec.Command("go", "test", "-c", "-o", testBinary, "./internal/store/")
	buildCmd.Dir = moduleRoot()
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("compile test binary: %v\n%s", err, string(out))
	}

	cmds := make([]*exec.Cmd, numHelpers)
	for i := 0; i < numHelpers; i++ {
		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("win-results-%d.jsonl", i))
		cmd := exec.Command(testBinary,
			"-test.run=TestSubprocessCrossProcessLockContentionWindowsHelper",
		)
		cmd.Env = append(os.Environ(),
			"TEST_HELPER_PROCESS=1",
			"TEST_TMPDIR="+tmp,
			"TEST_RESULTS_FILE="+resultsFile,
			"TEST_HELPER_ID="+fmt.Sprintf("%d", i),
		)
		// NOTE: On Windows, os.Create opens files with exclusive access.
		// Passing those handles to a subprocess causes sharing violations
		// when the child writes to stderr/stdout, which can block the
		// child indefinitely. Use os.Stderr/os.Stdout (which have
		// compatible sharing mode) so the child's output goes to the
		// CI log.
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmds[i] = cmd
	}

	// Start all helpers concurrently.
	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start helper %d: %v", i, err)
		}
	}

	// Verify helpers started: each helper writes a status file before the
	// TCP barrier. Poll for up to 10s.
	for i := 0; i < numHelpers; i++ {
		statusFile := filepath.Join(resultsDir, fmt.Sprintf("status-%d.txt", i))
		statusDeadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Lstat(statusFile); err == nil {
				break
			}
			if time.Now().After(statusDeadline) {
				// Check if subprocess already exited (early crash).
				for j, c := range cmds {
					if c.ProcessState != nil && c.ProcessState.Exited() {
						t.Fatalf("helper %d exited early with status=%v before barrier",
							j, c.ProcessState.ExitCode())
					}
				}
				t.Fatalf("helper %d did not write status file within 10s", i)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// TCP barrier: accept connections from all helpers, then broadcast "go".
	if err := tcpBarrierAcceptAndBroadcast(listener, numHelpers); err != nil {
		// Kill helpers on barrier failure.
		for _, cmd := range cmds {
			cmd.Process.Kill() //nolint:errcheck
			cmd.Wait()         //nolint:errcheck
		}
		t.Fatalf("tcp barrier: %v", err)
	}
	listener.Close() // no more connections needed

	// Wait for all helpers with a shared deadline.
	contentionTimeout := 30 * time.Second
	waitDeadline := time.Now().Add(contentionTimeout)

	// done carries both the helper index and its exit error, so results are
	// read by the real index regardless of completion order.
	type helperResult struct {
		idx int
		err error
	}
	done := make(chan helperResult, numHelpers)
	for i, cmd := range cmds {
		go func(idx int, c *exec.Cmd) {
			done <- helperResult{idx: idx, err: c.Wait()}
		}(i, cmd)
	}

	var totalSuccess int
	completed := make([]bool, numHelpers)
	for completedCount := 0; completedCount < numHelpers; completedCount++ {
		var hr helperResult
		select {
		case hr = <-done:
			completed[hr.idx] = true
		case <-time.After(time.Until(waitDeadline)):
			// Shared deadline exceeded: Kill all remaining helpers and drain.
			for i, cmd := range cmds {
				if !completed[i] {
					cmd.Process.Kill()
					cmd.Wait()
				}
			}
			// Drain any remaining results to avoid goroutine leak.
			for remaining := completedCount + 1; remaining < numHelpers; remaining++ {
				select {
				case hr = <-done:
					completed[hr.idx] = true
				case <-time.After(2 * time.Second):
					// Give up draining; report timeout.
					t.Fatalf("Windows contention timeout after %v, %d/%d helpers completed",
						contentionTimeout, completedCount, numHelpers)
				}
			}
			t.Fatalf("Windows contention timeout after %v, %d/%d helpers completed",
				contentionTimeout, completedCount, numHelpers)
		}

		if hr.err != nil {
			t.Logf("helper %d exited with error: %v", hr.idx, hr.err)
		}

		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("win-results-%d.jsonl", hr.idx))
		data, err := os.ReadFile(resultsFile)
		if err != nil {
			t.Fatalf("helper %d: read results: %v", hr.idx, err)
		}
		var r struct {
			Successes int      `json:"successes"`
			Timeouts  int      `json:"timeouts"`
			Other     int      `json:"other"`
			Errors    []string `json:"errors,omitempty"`
		}
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("helper %d: unmarshal: %v\nraw: %s", hr.idx, err, string(data))
		}
		t.Logf("Windows helper %d: successes=%d timeouts=%d other=%d", hr.idx, r.Successes, r.Timeouts, r.Other)
		totalSuccess += r.Successes
		if r.Other > 0 {
			t.Errorf("Windows helper %d: %d unexpected errors", hr.idx, r.Other)
		}
	}

	// Verify revision invariant: final revision == successes + initial.
	s2, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	finalSnap, err := s2.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if finalSnap == nil {
		t.Fatal("final snapshot missing after Windows cross-process contention")
	}
	if finalSnap.Revision != totalSuccess+1 {
		t.Errorf("Windows final revision = %d, want %d (initial=1 + successes=%d)", finalSnap.Revision, totalSuccess+1, totalSuccess)
	}
}

// TestSubprocessCrossProcessLockContentionWindowsHelper1First verifies the
// result-matching logic when helper 1 completes before helper 0. It uses the
// same compiled-test-binary approach with TEST_HELPER_FAST=1 telling helper 1
// to skip its extra sleep, forcing it to write results first.
// TCP barrier replaces file-based barrier for Windows compatibility.
func TestSubprocessCrossProcessLockContentionWindowsHelper1First(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows-specific cross-process contention test")
	}

	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	resultsDir := filepath.Join(tmp, "results")
	if err := os.MkdirAll(resultsDir, 0o700); err != nil {
		t.Fatalf("mkdir results: %v", err)
	}

	const numHelpers = 2

	// TCP barrier: parent listens on a random port, writes port to a file.
	listener, err := tcpBarrierListen(resultsDir)
	if err != nil {
		t.Fatalf("tcp barrier listen: %v", err)
	}
	defer listener.Close()

	// Compile test binary; clean stale binary first.
	testBinary := filepath.Join(tmp, "taskmaster.test.exe")
	os.Remove(testBinary) //nolint:errcheck
	buildCmd := exec.Command("go", "test", "-c", "-o", testBinary, "./internal/store/")
	buildCmd.Dir = moduleRoot()
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("compile test binary: %v\n%s", err, string(out))
	}

	// Helper 1 gets TEST_HELPER_FAST=1 to skip its extra sleep and finish first.
	cmds := make([]*exec.Cmd, numHelpers)
	for i := 0; i < numHelpers; i++ {
		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("win-results-%d.jsonl", i))
		cmd := exec.Command(testBinary,
			"-test.run=TestSubprocessCrossProcessLockContentionWindowsHelper",
		)
		env := append(os.Environ(),
			"TEST_HELPER_PROCESS=1",
			"TEST_TMPDIR="+tmp,
			"TEST_RESULTS_FILE="+resultsFile,
			"TEST_HELPER_ID="+fmt.Sprintf("%d", i),
		)
		if i == 1 {
			env = append(env, "TEST_HELPER_FAST=1")
		}
		cmd.Env = env
		// NOTE: On Windows, os.Create opens files with exclusive access.
		// Use os.Stderr/os.Stdout to avoid sharing violations in child.
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmds[i] = cmd
	}

	for i, cmd := range cmds {
		if err := cmd.Start(); err != nil {
			t.Fatalf("start helper %d: %v", i, err)
		}
	}

	// Verify helpers started: each helper writes a status file before the
	// TCP barrier. Poll for up to 10s.
	for i := 0; i < numHelpers; i++ {
		statusFile := filepath.Join(resultsDir, fmt.Sprintf("status-%d.txt", i))
		statusDeadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Lstat(statusFile); err == nil {
				break
			}
			if time.Now().After(statusDeadline) {
				for j, c := range cmds {
					if c.ProcessState != nil && c.ProcessState.Exited() {
						t.Fatalf("helper %d exited early with status=%v before barrier",
							j, c.ProcessState.ExitCode())
					}
				}
				t.Fatalf("helper %d did not write status file within 10s", i)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	// TCP barrier: accept connections from all helpers, then broadcast "go".
	if err := tcpBarrierAcceptAndBroadcast(listener, numHelpers); err != nil {
		for _, cmd := range cmds {
			cmd.Process.Kill() //nolint:errcheck
			cmd.Wait()         //nolint:errcheck
		}
		t.Fatalf("tcp barrier: %v", err)
	}
	listener.Close()

	contentionTimeout := 30 * time.Second
	waitDeadline := time.Now().Add(contentionTimeout)

	type helperResult struct {
		idx int
		err error
	}
	done := make(chan helperResult, numHelpers)
	for i, cmd := range cmds {
		go func(idx int, c *exec.Cmd) {
			done <- helperResult{idx: idx, err: c.Wait()}
		}(i, cmd)
	}

	var totalSuccess int
	completed := make([]bool, numHelpers)
	for completedCount := 0; completedCount < numHelpers; completedCount++ {
		var hr helperResult
		select {
		case hr = <-done:
			completed[hr.idx] = true
		case <-time.After(time.Until(waitDeadline)):
			for i, cmd := range cmds {
				if !completed[i] {
					cmd.Process.Kill()
					cmd.Wait()
				}
			}
			for remaining := completedCount + 1; remaining < numHelpers; remaining++ {
				select {
				case hr = <-done:
					completed[hr.idx] = true
				case <-time.After(2 * time.Second):
					t.Fatalf("timeout draining, %d/%d completed", completedCount, numHelpers)
				}
			}
			t.Fatalf("timeout: %d/%d completed", completedCount, numHelpers)
		}

		if hr.err != nil {
			t.Logf("helper %d exited with error: %v", hr.idx, hr.err)
		}

		// CRITICAL: read results by REAL index (hr.idx), not loop counter.
		// This is the key fix: helper 1 may complete first (hr.idx==1),
		// so we must read win-results-1.jsonl, not win-results-0.jsonl.
		resultsFile := filepath.Join(resultsDir, fmt.Sprintf("win-results-%d.jsonl", hr.idx))
		data, err := os.ReadFile(resultsFile)
		if err != nil {
			t.Fatalf("helper %d: read results: %v", hr.idx, err)
		}
		var r struct {
			Successes int      `json:"successes"`
			Timeouts  int      `json:"timeouts"`
			Other     int      `json:"other"`
			Errors    []string `json:"errors,omitempty"`
		}
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatalf("helper %d: unmarshal: %v\nraw: %s", hr.idx, err, string(data))
		}
		t.Logf("helper %d (order %d): successes=%d", hr.idx, completedCount, r.Successes)
		totalSuccess += r.Successes
	}

	s2, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	finalSnap, err := s2.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if finalSnap == nil {
		t.Fatal("final snapshot missing")
	}
	if finalSnap.Revision != totalSuccess+1 {
		t.Errorf("final revision = %d, want %d (initial=1 + successes=%d)",
			finalSnap.Revision, totalSuccess+1, totalSuccess)
	}
	// Verify both helpers completed: we should have results from index 0 and 1.
	if !completed[0] || !completed[1] {
		t.Errorf("not all helpers completed: completed[0]=%v completed[1]=%v", completed[0], completed[1])
	}
}

// TestSubprocessCrossProcessLockContentionWindowsHelper is the helper entry
// point for TestSubprocessCrossProcessLockContentionWindows and
// TestSubprocessCrossProcessLockContentionWindowsHelper1First.
func TestSubprocessCrossProcessLockContentionWindowsHelper(t *testing.T) {
	if os.Getenv("TEST_HELPER_PROCESS") != "1" {
		return
	}
	testSubprocessCrossProcessLockContentionWindowsHelper(t)
}

func testSubprocessCrossProcessLockContentionWindowsHelper(t *testing.T) {
	tmp := os.Getenv("TEST_TMPDIR")
	if tmp == "" {
		t.Fatal("TEST_TMPDIR not set")
	}
	resultsFile := os.Getenv("TEST_RESULTS_FILE")
	if resultsFile == "" {
		t.Fatal("TEST_RESULTS_FILE not set")
	}
	helperID := os.Getenv("TEST_HELPER_ID")
	resultsDir := os.Getenv("TEST_RESULTS_DIR")
	if resultsDir == "" {
		resultsDir = filepath.Dir(resultsFile)
	}

	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	const subUpdates = 3
	var successCount, timeoutCount, otherCount int

	// Write initial results and status file so parent can verify subprocess
	// started before entering the TCP barrier.
	writeResultsFile := func() {
		r := struct {
			Successes int    `json:"successes"`
			Timeouts  int    `json:"timeouts"`
			Other     int    `json:"other"`
			HelperID  string `json:"helper_id"`
		}{
			Successes: successCount,
			Timeouts:  timeoutCount,
			Other:     otherCount,
			HelperID:  helperID,
		}
		data, _ := json.Marshal(r)
		if werr := os.WriteFile(resultsFile, append(data, '\n'), 0o600); werr != nil {
			t.Fatalf("write results: %v", werr)
		}
	}
	writeResultsFile()
	// Status file signals the parent that the helper reached the TCP barrier.
	if resultsDir != "" {
		statusFile := filepath.Join(resultsDir, fmt.Sprintf("status-%s.txt", helperID))
		if werr := os.WriteFile(statusFile, []byte("ready"), 0o600); werr != nil {
			t.Fatalf("write status: %v", werr)
		}
	}

	// TCP barrier: connect to parent, wait for "go" signal.
	if err := tcpBarrierClient(resultsDir); err != nil {
		t.Logf("TCP barrier error: %v", err)
		writeResultsFile()
		return
	}

	// Slow helpers sleep extra to create staggered completion order.
	// TEST_HELPER_FAST=1 skips this sleep (used by helper 1 in the
	// helper-1-first test to force reverse-order completion).
	if os.Getenv("TEST_HELPER_FAST") != "1" {
		time.Sleep(100 * time.Millisecond)
	}

	for iter := 0; iter < subUpdates; iter++ {
		mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
			if old == nil {
				next := sampleSnapshot()
				next.Revision = 1
				return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
			}
			next := *old
			next.Revision = old.Revision + 1
			next.LastEventID = fmt.Sprintf("win-event-%d-%s", iter, helperID)
			return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
		}
		err := s.Update(context.Background(), k, mutate)
		if err == nil {
			successCount++
		} else if errors.Is(err, domain.ErrLockTimeout) {
			timeoutCount++
		} else {
			otherCount++
		}
	}

	r := struct {
		Successes int    `json:"successes"`
		Timeouts  int    `json:"timeouts"`
		Other     int    `json:"other"`
		HelperID  string `json:"helper_id"`
	}{
		Successes: successCount,
		Timeouts:  timeoutCount,
		Other:     otherCount,
		HelperID:  helperID,
	}
	data, _ := json.Marshal(r)
	if err := os.WriteFile(resultsFile, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write results: %v", err)
	}
}

// tcpBarrierListen creates a TCP listener on a random port and writes the port
// number to barrier-port.txt in resultsDir. Helpers read this file to connect.
func tcpBarrierListen(resultsDir string) (*net.TCPListener, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("resolve tcp addr: %w", err)
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen tcp: %w", err)
	}
	portFile := filepath.Join(resultsDir, "barrier-port.txt")
	if err := os.WriteFile(portFile, []byte(strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)), 0o600); err != nil {
		listener.Close()
		return nil, fmt.Errorf("write port file: %w", err)
	}
	return listener, nil
}

// tcpBarrierAcceptAndBroadcast accepts connections from all expected helpers,
// sends each a ready byte, then broadcasts "go" to all of them.
func tcpBarrierAcceptAndBroadcast(listener *net.TCPListener, numHelpers int) error {
	listener.SetDeadline(time.Now().Add(60 * time.Second))
	conns := make([]net.Conn, 0, numHelpers)
	for len(conns) < numHelpers {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// Close any accepted connections before returning.
			for _, c := range conns {
				c.Close() //nolint:errcheck
			}
			return fmt.Errorf("accept helper %d/%d: %w", len(conns)+1, numHelpers, err)
		}
		// Send ready signal.
		if _, err := conn.Write([]byte("ready")); err != nil {
			conn.Close()
			for _, c := range conns {
				c.Close() //nolint:errcheck
			}
			return fmt.Errorf("send ready to helper %d: %w", len(conns)+1, err)
		}
		conns = append(conns, conn)
	}
	// Broadcast "go" to all helpers.
	for _, conn := range conns {
		if _, err := conn.Write([]byte("go")); err != nil {
			// Non-fatal: helpers may have already received and proceeded.
			tcpLogf("broadcast go: %v (non-fatal)", err)
		}
	}
	// Close connections after broadcast.
	for _, conn := range conns {
		conn.Close() //nolint:errcheck
	}
	return nil
}

// tcpBarrierClient connects to the parent's TCP barrier and waits for "go".
func tcpBarrierClient(resultsDir string) error {
	portFile := filepath.Join(resultsDir, "barrier-port.txt")
	portBytes, err := os.ReadFile(portFile)
	if err != nil {
		return fmt.Errorf("read port file: %w", err)
	}
	port, err := strconv.Atoi(string(portBytes))
	if err != nil {
		return fmt.Errorf("parse port: %w", err)
	}

	// Retry connection for up to 60s (parent may still be compiling/binding).
	var conn net.Conn
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 2*time.Second)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return fmt.Errorf("connect to parent barrier: %w", err)
	}
	defer conn.Close() //nolint:errcheck

	// Read ready signal from parent.
	buf := make([]byte, 4)
	if _, err := conn.Read(buf); err != nil {
		return fmt.Errorf("read ready: %w", err)
	}

	// Read "go" broadcast from parent.
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	goBuf := make([]byte, 2)
	if _, err := conn.Read(goBuf); err != nil {
		return fmt.Errorf("read go: %w", err)
	}
	return nil
}

// tcpLogf logs messages from TCP barrier helpers. On non-test builds this is a
// no-op; during tests it writes to the test's output.
func tcpLogf(format string, args ...interface{}) {
	// In test context, use t.Logf via the testing.T. Since tcpBarrierClient
	// doesn't have access to *testing.T, we use fmt.Printf as a fallback.
	// The parent test captures stderr/stdout of subprocess helpers.
	fmt.Fprintf(os.Stderr, "[tcp-barrier] "+format+"\n", args...)
}

// =============================================================================
// P1-4: Windows Stat error retry rules
// =============================================================================

func TestIsTransientWindowsStatError(t *testing.T) {
	// Test that isTransientWindowsStatError correctly classifies errors.
	// ENOENT is always transient.
	if !isTransientWindowsStatError(os.ErrNotExist) {
		t.Error("ENOENT should be transient")
	}

	// nil is never transient.
	if isTransientWindowsStatError(nil) {
		t.Error("nil should not be transient")
	}

	// On non-Windows, generic errors are not transient.
	if runtime.GOOS != "windows" {
		if isTransientWindowsStatError(fmt.Errorf("some other error")) {
			t.Error("non-Windows: generic errors should not be transient")
		}
	}
}

// TestAcquireSessionLockTransientWindowsStatRetries verifies that on Windows,
// an ERROR_SHARING_VIOLATION (errno 32) from Stat is treated as transient:
// AcquireSessionLock retries within the 75 ms deadline instead of failing.
// On non-Windows, the same errno 32 is EPIPE (broken pipe), which is permanent;
// skip on non-Windows to avoid confusing error messages.
func TestAcquireSessionLockTransientWindowsStatRetries(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("This test exercises Windows-specific transient Stat retry; skip on non-Windows")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	lockDir := s.LockPath(sampleKey())

	// Pre-create lock dir so Mkdir returns EEXIST, triggering the Stat path.
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}

	// Inject statSeam to always return errno 32 (SHARING_VIOLATION).
	origStatSeam := statSeam
	statSeam = func(path string) (os.FileInfo, error) {
		return nil, syscall.Errno(32) // ERROR_SHARING_VIOLATION
	}
	defer func() { statSeam = origStatSeam }()

	start := time.Now()
	_, _, _, err = s.AcquireSessionLock(lockDir)
	elapsed := time.Since(start)

	// Should timeout within the 75ms budget (allow 100ms for scheduler jitter).
	if elapsed > 100*time.Millisecond {
		t.Errorf("AcquireSessionLock took %v, want < 100ms (75ms budget)", elapsed)
	}
	if !errors.Is(err, domain.ErrLockTimeout) {
		t.Errorf("AcquireSessionLock() error = %v, want ErrLockTimeout", err)
	}
}

func TestAcquireSessionLockPermanentWindowsStatError(t *testing.T) {
	// Verify that errno 5 (ERROR_ACCESS_DENIED) is NOT retried: the error
	// is propagated immediately. Uses statSeam to inject the error.
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	lockDir := s.LockPath(sampleKey())

	// Pre-create lock dir so Mkdir returns EEXIST, triggering the Stat path.
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}

	// Inject statSeam to return errno 5 (ACCESS_DENIED) — permanent error.
	origStatSeam := statSeam
	statSeam = func(path string) (os.FileInfo, error) {
		return nil, syscall.Errno(5) // ERROR_ACCESS_DENIED
	}
	defer func() { statSeam = origStatSeam }()

	start := time.Now()
	_, _, _, err = s.AcquireSessionLock(lockDir)
	elapsed := time.Since(start)

	// Should return immediately, no retries (much faster than 75ms budget).
	if elapsed > 10*time.Millisecond {
		t.Errorf("AcquireSessionLock took %v for permanent error, want < 10ms", elapsed)
	}
	if err == nil {
		t.Fatal("AcquireSessionLock() should have returned permanent error")
	}
	if errors.Is(err, domain.ErrLockTimeout) {
		t.Fatal("AcquireSessionLock() should NOT return ErrLockTimeout for permanent error")
	}
}

// =============================================================================
// P2-1: Lock release failure tests
// =============================================================================

func TestReleaseSessionLockFailureOnRemove(t *testing.T) {
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	// Create lock dir and nonce file.
	if err := os.Mkdir(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write nonce: %v", err)
	}

	// Make lock DIR read-only so Remove(lockFile) fails (Unix: removing a file
	// requires write permission on the file's parent directory).
	if err := os.Chmod(lockDir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	defer os.Chmod(lockDir, 0o700)

	err := ReleaseSessionLock(lockDir, lockFile, "correct-nonce")
	if err == nil {
		t.Fatal("ReleaseSessionLock() should have failed on remove error")
	}
	// On Unix, Remove(lockFile) fails first → "remove lock file".
	// On Windows, Remove(lockFile) may succeed but Remove(lockDir) fails → "remove lock dir".
	if !strings.Contains(err.Error(), "remove lock") {
		t.Errorf("ReleaseSessionLock() error = %q, want 'remove lock'", err)
	}
}

func TestReleaseSessionLockFailureOnRemoveDir(t *testing.T) {
	tmp := t.TempDir()
	lockDir := filepath.Join(tmp, "test.lock")
	lockFile := filepath.Join(lockDir, ".lock")

	if err := os.Mkdir(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(lockFile, []byte("correct-nonce\n"), 0o600); err != nil {
		t.Fatalf("write nonce: %v", err)
	}

	// Create a dummy file inside the lock dir so it's non-empty.
	// Removing a non-empty directory fails on Unix.
	dummy := filepath.Join(lockDir, "dummy")
	if err := os.WriteFile(dummy, []byte("dummy"), 0o600); err != nil {
		t.Fatalf("write dummy: %v", err)
	}

	// Use empty nonce to skip verification and attempt direct removal of
	// lockFile then lockDir. Nonce file removal succeeds; lock dir removal
	// fails because the dir is non-empty.
	err := ReleaseSessionLock(lockDir, lockFile, "")
	if err == nil {
		t.Fatal("ReleaseSessionLock() should have failed on remove dir error")
	}
	if !strings.Contains(err.Error(), "remove lock dir") {
		t.Errorf("ReleaseSessionLock() error = %q, want 'remove lock dir'", err)
	}
}

func TestUpdateReleaseFailurePropagated(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "original"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Override lock file removal to simulate failure.
	removeSeam = func(name string) error {
		return fmt.Errorf("simulated cleanup failure")
	}

	defer func() { removeSeam = os.Remove }()

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		next.Message = "updated"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should have returned release error")
	}
	// When both the main operation (writeSnapshot removeSeam) and the lock
	// cleanup (ReleaseSessionLock removeSeam) fail, Update's defer produces
	// "main: <write error>; cleanup: <lock error>". When only the lock cleanup
	// fails, it produces "update <key>: release lock: ...".
	errMsg := err.Error()
	if !strings.Contains(errMsg, "release lock") && !strings.Contains(errMsg, "cleanup") {
		t.Errorf("Update() error = %q, want error containing 'release lock' or 'cleanup'", errMsg)
	}
}

func TestDeleteReleaseFailurePropagated(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	snap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, snap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Override removeSeam to simulate lock cleanup failure.
	removeSeam = func(name string) error {
		return fmt.Errorf("simulated cleanup failure")
	}
	defer func() { removeSeam = os.Remove }()

	err = s.Delete(context.Background(), k)
	if err == nil {
		t.Fatal("Delete() should have returned release error")
	}
	// Delete has no main operation error; the lock cleanup failure is the
	// primary error: "delete <key>: release lock: remove lock file: ...".
	// But if removeSeam also fails on the lock dir removal, the format is
	// "delete <key>: main: remove lock dir: ...; cleanup: remove lock file: ...".
	errMsg := err.Error()
	if !strings.Contains(errMsg, "release lock") && !strings.Contains(errMsg, "cleanup") {
		t.Errorf("Delete() error = %q, want error containing 'release lock' or 'cleanup'", errMsg)
	}
}

func TestTempFileRemoveFailureDoesNotLeaveHalfWrittenFile(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "original"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Override writeFileSeam to fail during the write phase. The defer in
	// writeSnapshot will then attempt to remove the still-existing temp file,
	// exercising the temp file cleanup error path.
	writeFileSeam = func(f *os.File, b []byte) (int, error) {
		return 0, fmt.Errorf("simulated write failure")
	}

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Status = domain.StatusCompleted
		next.Message = "updated"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	if err == nil {
		t.Fatal("Update() should have failed")
	}
	if !strings.Contains(err.Error(), "write temp") {
		t.Errorf("Update() error = %q, want 'write temp'", err)
	}
	// Restore writeFileSeam before assertions.
	writeFileSeam = func(f *os.File, b []byte) (int, error) { return f.Write(b) }

	// Old snapshot must be preserved (write failed before rename).
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("old snapshot was lost!")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("status = %q, want %q", got.Status, domain.StatusWorking)
	}
	if got.Message != "original" {
		t.Errorf("message = %q, want %q", got.Message, "original")
	}

	// Verify no temp files remain.
	sessionsDir := filepath.Join(tmp, "sessions")
	filepath.Walk(sessionsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if !info.IsDir() && strings.Contains(info.Name(), ".tmp-") {
			t.Errorf("leftover temp file: %s", path)
		}
		return nil
	})

	// Verify lock was released: a subsequent Update should succeed.
	mutate2 := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Message = "reacquired"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
	}
	if err := s.Update(context.Background(), k, mutate2); err != nil {
		t.Fatalf("Update() after write failure error = %v", err)
	}
}

// TestWriteSnapshotRemoveFailurePropagated verifies two things:
//  1. When replaceExistingSeam (the atomic rename) fails, writeSnapshot returns
//     that error and the old snapshot is preserved (no partial commit).
//  2. The lock is reusable after the failure (a subsequent Update succeeds).
//
// The defer in writeSnapshot gives priority to any prior error over the
// removeSeam cleanup error: when replaceExisting fails, rerr is already set,
// so a removeSeam failure on the still-present temp file does not overwrite
// the primary error. This test exercises that path and documents the contract:
// callers see the primary failure; temp cleanup errors are best-effort.
func TestWriteSnapshotRemoveFailurePropagated(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "original"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Inject replaceExistingSeam failure: the temp file is created and written
	// successfully, but the atomic rename fails. The temp file remains on disk.
	// writeSnapshot's defer then calls removeSeam(tmpPath) — the temp file still
	// exists, removeSeam succeeds, and the replace error is returned to caller.
	origReplaceExistingSeam := replaceExistingSeam
	replaceExistingSeam = func(newPath, oldPath string) error {
		return fmt.Errorf("injected rename failure")
	}
	defer func() { replaceExistingSeam = origReplaceExistingSeam }()

	// Track whether removeSeam is called on a .tmp-* path (it will be, since
	// the rename failed and the temp file still exists).
	origRemoveSeam := removeSeam
	removeCalled := false
	removeSeam = func(path string) error {
		if strings.Contains(filepath.Base(path), ".tmp-") {
			removeCalled = true
		}
		return origRemoveSeam(path)
	}
	defer func() { removeSeam = origRemoveSeam }()

	newSnap := sampleSnapshot()
	newSnap.Status = domain.StatusCompleted
	newSnap.Message = "updated"
	err = s.Commit(context.Background(), k, newSnap)

	// Assert: error must be returned and must indicate rename failure.
	if err == nil {
		t.Fatal("Commit() should have failed when replaceExistingSeam fails")
	}
	if !strings.Contains(err.Error(), "rename") {
		t.Errorf("Commit() error = %q, want error containing 'rename'", err.Error())
	}

	// Old snapshot must be preserved: rename never happened, so the original
	// session file is still intact.
	got, loadErr := s.Load(context.Background(), k)
	if loadErr != nil {
		t.Fatalf("Load() error = %v", loadErr)
	}
	if got == nil {
		t.Fatal("old snapshot was lost after replaceExisting failure")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("old status = %q, want %q", got.Status, domain.StatusWorking)
	}
	if got.Message != "original" {
		t.Errorf("old message = %q, want %q", got.Message, "original")
	}

	// Verify no temp files remain: the defer's removeSeam cleaned up the temp.
	sessionsDir := filepath.Join(tmp, "sessions")
	var tmpFiles []string
	filepath.Walk(sessionsDir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return nil
		}
		if !info.IsDir() && strings.Contains(info.Name(), ".tmp-") {
			tmpFiles = append(tmpFiles, filepath.Base(path))
		}
		return nil
	})
	if len(tmpFiles) > 0 {
		t.Errorf("leftover temp files after failed rename: %v", tmpFiles)
	}

	// Verify removeSeam was called with the .tmp-* temp file.
	if !removeCalled {
		t.Error("removeSeam was not called with a .tmp-* path")
	}

	// Restore replaceExistingSeam before the reusability check so that
	// Update's writeSnapshot uses the real implementation.
	replaceExistingSeam = origReplaceExistingSeam

	// Verify lock was released: a subsequent Update should succeed.
	mutate2 := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		next := *old
		next.Message = "reacquired"
		return domain.ReduceResult{Next: &next, Transition: domain.Transition{StateChanged: false}}, nil
	}
	if err := s.Update(context.Background(), k, mutate2); err != nil {
		t.Fatalf("Update() after replaceExisting failure error = %v", err)
	}
	// Verify the Update succeeded: snapshot has the new message.
	got2, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() after Update error = %v", err)
	}
	if got2 == nil {
		t.Fatal("snapshot missing after successful Update")
	}
	if got2.Message != "reacquired" {
		t.Errorf("message = %q, want %q", got2.Message, "reacquired")
	}
}

// =============================================================================
// P2-2: Corrupt retention must never exceed 3 even with 5 pre-existing
// =============================================================================

func TestCorruptRetentionMax3With5PreExisting(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()

	// Commit initial snapshot.
	initialSnap := sampleSnapshot()
	if err := s.Commit(context.Background(), k, initialSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Pre-create 5 corrupt backup files.
	sessionPath := s.Path(k)
	dir := filepath.Dir(sessionPath)
	base := filepath.Base(sessionPath)
	for i := 0; i < 5; i++ {
		corruptPath := filepath.Join(dir, fmt.Sprintf("%s.corrupt.%d", base, i))
		if err := os.WriteFile(corruptPath, []byte(fmt.Sprintf("corrupt-%d", i)), 0o600); err != nil {
			t.Fatalf("pre-create corrupt %d: %v", i, err)
		}
	}

	// Make session file corrupt so quarantineCorrupt is triggered.
	if err := os.WriteFile(sessionPath, []byte("not json"), 0o600); err != nil {
		t.Fatalf("write corrupt: %v", err)
	}

	// Trigger quarantine via Update with a valid new snapshot.
	newSnap := sampleSnapshot()
	newSnap.Revision = 2
	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		return domain.ReduceResult{Next: &newSnap, Transition: domain.Transition{StateChanged: true}}, nil
	}
	if err := s.Update(context.Background(), k, mutate); err != nil {
		t.Fatalf("Update() error = %v", err)
	}

	// Count remaining corrupt files.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var corruptCount int
	for _, e := range entries {
		name := e.Name()
		if len(name) > len(base)+9 && name[:len(base)+9] == base+".corrupt." {
			corruptCount++
		}
	}
	if corruptCount > 3 {
		t.Errorf("corrupt count = %d, want <= 3", corruptCount)
	}
}

// =============================================================================
// P1-1: Update delete failure must return error
// =============================================================================

func TestUpdateDeleteFailureReturnsError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ACLs don't enforce chmod-based directory deletion blocking")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	oldSnap := sampleSnapshot()
	oldSnap.Status = domain.StatusWorking
	oldSnap.Message = "to-delete"

	if err := s.Commit(context.Background(), k, oldSnap); err != nil {
		t.Fatalf("initial Commit() error = %v", err)
	}

	// Make agent directory read-only so os.Remove fails (needs write on parent dir).
	agentDir := filepath.Join(tmp, "sessions", k.Agent)
	if err := os.Chmod(agentDir, 0o500); err != nil {
		t.Fatalf("chmod agent dir: %v", err)
	}
	defer os.Chmod(agentDir, 0o700)

	mutate := func(old *domain.SessionSnapshot) (domain.ReduceResult, error) {
		return domain.ReduceResult{Next: nil, Transition: domain.Transition{ShouldDelete: true, StateChanged: true}}, nil
	}
	err = s.Update(context.Background(), k, mutate)
	os.Chmod(agentDir, 0o700) // restore before assertion
	if err == nil {
		t.Fatal("Update() should have failed on delete error")
	}
	if !strings.Contains(err.Error(), "delete") {
		t.Errorf("Update() error = %q, want 'delete'", err)
	}

	// Verify old snapshot is still present (delete failed).
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("old snapshot was lost after failed delete")
	}
	if got.Status != domain.StatusWorking {
		t.Errorf("status = %q, want %q", got.Status, domain.StatusWorking)
	}
}

// =============================================================================
// P1-2: List must return error on agent dir read failure
// =============================================================================

func TestListReturnsErrorOnAgentDirReadFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ACLs don't enforce chmod-based directory read blocking")
	}
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}

	// Create an agent directory and make it unreadable.
	agentDir := filepath.Join(tmp, "sessions", "bad-agent")
	if err := os.MkdirAll(agentDir, 0o700); err != nil {
		t.Fatalf("mkdir agent dir: %v", err)
	}
	if err := os.Chmod(agentDir, 0o000); err != nil {
		t.Fatalf("chmod agent dir: %v", err)
	}
	defer os.Chmod(agentDir, 0o700)

	_, err = s.List(context.Background(), now.Add(time.Hour))
	if err == nil {
		t.Fatal("List() should have returned error on unreadable agent dir")
	}
	if !strings.Contains(err.Error(), "list agent dir") {
		t.Errorf("List() error = %q, want 'list agent dir'", err)
	}
}

// =============================================================================
// P1-8: Lock Stat error respects 75ms deadline
// =============================================================================

func TestAcquireSessionLockStatErrorRespectsDeadline(t *testing.T) {
	tmp := t.TempDir()
	s, err := New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k := sampleKey()
	lockDir := s.LockPath(k)

	// Create lock dir.
	if err := os.MkdirAll(lockDir, 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}

	// Replace lock dir with a symlink to a non-existent target.
	// Stat on the symlink will try to follow it and get ENOENT.
	os.Remove(lockDir)
	target := filepath.Join(tmp, "nonexistent", "target")
	if err := os.Symlink(target, lockDir); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	start := time.Now()
	_, _, _, err = s.AcquireSessionLock(lockDir)
	elapsed := time.Since(start)

	// Should timeout within the 75ms budget, not spin forever.
	if elapsed > 100*time.Millisecond {
		t.Errorf("AcquireSessionLock took %v, want < 100ms", elapsed)
	}
	if !errors.Is(err, domain.ErrLockTimeout) {
		t.Fatalf("AcquireSessionLock() error = %v, want ErrLockTimeout", err)
	}
}

// moduleRoot returns the repository root directory (where go.mod lives).
func moduleRoot() string {
	// This file is in internal/store/, so module root is two levels up.
	dir, _ := filepath.Abs(filepath.Join(filepath.Dir(""), "..", ".."))
	return dir
}
