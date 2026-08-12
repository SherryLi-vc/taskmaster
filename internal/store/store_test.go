package store_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
	"github.com/taskmaster-dev/taskmaster/internal/store"
)

var now = time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)

func sampleKey() domain.SessionKey {
	return domain.SessionKey{
		Agent:      "claude",
		SessionID:  "sess-123",
		SessionIDH: domain.SessionHash("sess-123"),
	}
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

// --- Path resolution ---

func TestStorePathResolution(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	got := s.Root()
	if got != tmp {
		t.Errorf("Root() = %q, want %q", got, tmp)
	}
}

func TestStoreCreatesRequiredDirectories(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func TestStoreAgentSubdir(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func TestStoreLockPath(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

// --- Load / Commit / Delete round-trip ---

func TestStoreCommitAndLoad(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func TestStoreLoadMissing(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func TestStoreDelete(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func TestStoreDeleteMissing(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	// Delete on non-existent key should succeed (idempotent).
	if err := s.Delete(context.Background(), sampleKey()); err != nil {
		t.Fatalf("Delete() on missing session error = %v", err)
	}
}

// --- Corrupted snapshot handling ---

func TestStoreLoadCorruptSnapshot(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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

func isCorruptError(err error) bool {
	return err != nil && (errors.Is(err, domain.ErrCorruptState) || errors.Is(err, domain.ErrInvalidInput))
}

func TestStoreList(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	k1 := sampleKey()
	snap1 := sampleSnapshot()
	// expires at now + 15m (working TTL)
	snap1.ExpiresAt = now.Add(15 * time.Minute)
	if err := s.Commit(context.Background(), k1, snap1); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	// Second session that has expired
	k2 := sampleKey()
	k2.SessionID = "sess-expired"
	k2.SessionIDH = domain.SessionHash("sess-expired")
	snap2 := sampleSnapshot()
	snap2.SessionID = "sess-expired"
	snap2.SessionIDHash = domain.SessionHash("sess-expired")
	snap2.ExpiresAt = now.Add(-1 * time.Hour) // expired
	if err := s.Commit(context.Background(), k2, snap2); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	list, err := s.List(context.Background(), now)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("List() returned %d sessions, want 1", len(list))
	}
	if list[0].SessionID != "sess-123" {
		t.Errorf("List()[0].SessionID = %q, want sess-123", list[0].SessionID)
	}
}

func TestStoreListEmpty(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	list, err := s.List(context.Background(), now)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list) != 0 {
		t.Errorf("List() returned %d sessions, want 0", len(list))
	}
}

// --- Concurrent commits ---

func TestStoreConcurrentCommits(t *testing.T) {
	tmp := t.TempDir()
	s, err := store.New(tmp)
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
			if err := s.Commit(context.Background(), k, snap); err != nil {
				t.Errorf("Commit goroutine %d error = %v", n, err)
			}
		}(i)
	}
	wg.Wait()
	// Should have at least one snapshot.
	got, err := s.Load(context.Background(), k)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got == nil {
		t.Fatal("Load() = nil after concurrent commits, want snapshot")
	}
}
