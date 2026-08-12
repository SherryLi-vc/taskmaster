package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

const (
	// maxLockJitterMs is the total jitter budget for contended locks.
	maxLockJitterMs = 75 * time.Millisecond
	// staleLock is the mtime threshold after which a lock is considered abandoned.
	staleLock = 2 * time.Second
)

// Store persists agent session snapshots as per-platform JSON files.
type Store struct {
	root string
	mu   sync.Mutex
}

// New creates or opens a Store rooted at dir, creating required subdirectories.
func New(dir string) (*Store, error) {
	for _, sub := range []string{"sessions", "locks", "backups"} {
		p := filepath.Join(dir, sub)
		if err := os.MkdirAll(p, 0o700); err != nil {
			return nil, fmt.Errorf("%w: create %s: %v", err, sub, err)
		}
	}
	return &Store{root: dir}, nil
}

// Root returns the absolute path to the store root.
func (s *Store) Root() string { return s.root }

// Path returns the session file path for the given key.
func (s *Store) Path(k domain.SessionKey) string {
	return filepath.Join(s.root, "sessions", k.Agent, k.SessionIDH+".json")
}

// LockPath returns the lock directory path for the given key.
func (s *Store) LockPath(k domain.SessionKey) string {
	return filepath.Join(s.root, "locks", k.Agent+"-"+k.SessionIDH+".lock")
}

// Load reads and validates the session snapshot for k.
func (s *Store) Load(_ context.Context, k domain.SessionKey) (*domain.SessionSnapshot, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.Path(k))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("load %s: %w", k.String(), err)
	}
	var snap domain.SessionSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("%w: corrupt snapshot %s: %v", domain.ErrCorruptState, k.String(), err)
	}
	if err := snap.Validate(); err != nil {
		return nil, fmt.Errorf("%w: snapshot %s invalid: %v", domain.ErrCorruptState, k.String(), err)
	}
	return &snap, nil
}

// Commit atomically writes the snapshot under a short-lived lock.
// It returns ErrLockTimeout if the lock cannot be acquired within the jitter budget.
func (s *Store) Commit(_ context.Context, k domain.SessionKey, snap domain.SessionSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := k.Validate(); err != nil {
		return err
	}
	if err := snap.Validate(); err != nil {
		return err
	}

	lockDir := s.LockPath(k)
	sessionPath := s.Path(k)

	deadline := time.Now().Add(maxLockJitterMs)
	lockFile := filepath.Join(lockDir, ".lock")
	for {
		// Ensure lock dir exists before trying to create the marker file.
		if err := os.MkdirAll(lockDir, 0o700); err != nil {
			return fmt.Errorf("commit %s: lock mkdir: %w", k.String(), err)
		}
		// Try to create the marker file exclusively.
		f, err := os.Create(lockFile)
		if err == nil {
			f.Close()
			break // lock acquired
		}
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("commit %s: lock create: %w", k.String(), err)
		}
		// File exists — check if lock is stale.
		info, statErr := os.Stat(lockFile)
		if statErr == nil && time.Since(info.ModTime()) > staleLock {
			os.Remove(lockFile)
			continue // retry
		}
		// Not stale and we've exhausted the jitter budget.
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: commit %s", domain.ErrLockTimeout, k.String())
		}
		// Wait a bit before retrying.
		time.Sleep(5 * time.Millisecond)
	}

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(sessionPath), 0o700); err != nil {
		return fmt.Errorf("commit %s: mkdir: %w", k.String(), err)
	}

	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return fmt.Errorf("commit %s: marshal: %w", k.String(), err)
	}
	data = append(data, '\n')

	tmp := sessionPath + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("commit %s: write tmp: %w", k.String(), err)
	}

	// fsync file.
	if f, err := os.OpenFile(tmp, os.O_RDWR, 0o600); err == nil {
		f.Sync()
		f.Close()
	}

	if err := os.Rename(tmp, sessionPath); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("commit %s: rename: %w", k.String(), err)
	}

	// fsync parent (best-effort, platform-dependent).
	if runtime.GOOS != "windows" {
		if d, err := os.Open(filepath.Dir(sessionPath)); err == nil {
			d.Sync()
			d.Close()
		}
	}

	// Release lock.
	os.Remove(lockFile)
	os.Remove(lockDir)

	return nil
}

// Delete removes the session snapshot file.
func (s *Store) Delete(_ context.Context, k domain.SessionKey) error {
	if err := k.Validate(); err != nil {
		return err
	}
	path := s.Path(k)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("delete %s: %w", k.String(), err)
	}
	return nil
}

// List returns all snapshots whose ExpiresAt is after before.
func (s *Store) List(_ context.Context, before time.Time) ([]domain.SessionSnapshot, error) {
	base := filepath.Join(s.root, "sessions")
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []domain.SessionSnapshot
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		agent := entry.Name()
		agentDir := filepath.Join(base, agent)
		files, err := os.ReadDir(agentDir)
		if err != nil {
			continue
		}
		for _, f := range files {
			if filepath.Ext(f.Name()) != ".json" {
				continue
			}
			data, err := os.ReadFile(filepath.Join(agentDir, f.Name()))
			if err != nil {
				continue
			}
			var snap domain.SessionSnapshot
			if err := json.Unmarshal(data, &snap); err != nil {
				continue
			}
			if snap.ExpiresAt.After(before) {
				result = append(result, snap)
			}
		}
	}
	return result, nil
}
