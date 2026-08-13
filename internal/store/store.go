package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

// Store persists agent session snapshots as per-platform JSON files.
type Store struct {
	root string
}

// New creates or opens a Store rooted at dir, creating required subdirectories.
// It validates the root directory, tightens permissions, and rejects symlinks.
func New(dir string) (*Store, error) {
	// Finding #13: validate root directory.
	root, err := normalizeAndValidateRoot(dir)
	if err != nil {
		return nil, err
	}

	for _, sub := range []string{"sessions", "locks", "backups"} {
		p := filepath.Join(root, sub)
		// Check for symlink before MkdirAll to prevent symlink attacks.
		if info, err := os.Lstat(p); err == nil {
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("%s is a symlink, not a directory", sub)
			}
			if !info.IsDir() {
				return nil, fmt.Errorf("%s exists but is not a directory", sub)
			}
		}
		// Additional check after MkdirAll: a race could create a symlink
		// between our Lstat and another process's MkdirAll/Mkdir.
		if err := os.MkdirAll(p, 0o700); err != nil {
			return nil, fmt.Errorf("create %s: %w", sub, err)
		}
		var (
			info os.FileInfo
			err2 error
		)
		if info, err2 = os.Lstat(p); err2 != nil {
			return nil, fmt.Errorf("stat %s: %w", sub, err2)
		} else if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("%s is a symlink, not a directory", sub)
		} else if !info.IsDir() {
			return nil, fmt.Errorf("%s exists but is not a directory", sub)
		}
		if info.Mode().Perm() != 0o700 {
			if err := os.Chmod(p, 0o700); err != nil {
				return nil, fmt.Errorf("chmod %s to 0700: %w", sub, err)
			}
		}
	}
	return &Store{root: root}, nil
}

// normalizeAndValidateRoot converts dir to an absolute path and validates it:
//   - resolves to absolute path
//   - rejects symlinks
//   - rejects non-directories
//   - on Unix, enforces 0700 permissions
func normalizeAndValidateRoot(dir string) (string, error) {
	// Convert to absolute path.
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("abs %s: %w", dir, err)
	}

	// Check if root exists.
	info, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			// Root doesn't exist yet — create it with 0700.
			if err := os.MkdirAll(abs, 0o700); err != nil {
				return "", fmt.Errorf("create root %s: %w", abs, err)
			}
			return abs, nil
		}
		return "", fmt.Errorf("stat root %s: %w", abs, err)
	}

	// Reject symlinks at root.
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("root is a symlink: %s", abs)
	}

	// Reject non-directories.
	if !info.IsDir() {
		return "", fmt.Errorf("root exists but is not a directory: %s", abs)
	}

	// Enforce 0700 on Unix.
	if info.Mode().Perm() != 0o700 {
		if err := os.Chmod(abs, 0o700); err != nil {
			return "", fmt.Errorf("chmod root %s to 0700: %w", abs, err)
		}
	}

	return abs, nil
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
// If the snapshot file is corrupt, it returns ErrCorruptState.
func (s *Store) Load(_ context.Context, k domain.SessionKey) (*domain.SessionSnapshot, error) {
	if err := k.Validate(); err != nil {
		return nil, err
	}
	path := s.Path(k)
	// Finding #11: validate managed path.
	if err := validateManagedPath(s.root, filepath.Dir(path), false); err != nil {
		return nil, fmt.Errorf("load %s: %w", k.String(), err)
	}

	data, err := os.ReadFile(path)
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
	// Finding #9: verify key identity matches snapshot.
	if err := domain.VerifyKeySnapshotIdentity(k, &snap); err != nil {
		return nil, fmt.Errorf("%w: %s", domain.ErrCorruptState, err)
	}
	return &snap, nil
}

// Commit writes a snapshot directly to the session file under a per-session lock.
// It returns ErrLockTimeout if the lock cannot be acquired within the jitter budget.
// For transactional read-reduce-write, use Update instead.
func (s *Store) Commit(_ context.Context, k domain.SessionKey, snap domain.SessionSnapshot) error {
	if err := k.Validate(); err != nil {
		return err
	}
	if err := domain.VerifyKeySnapshotIdentity(k, &snap); err != nil {
		return fmt.Errorf("%w: %s", domain.ErrInvalidInput, err)
	}
	if err := snap.Validate(); err != nil {
		return err
	}

	lockDir := s.LockPath(k)
	sessionPath := s.Path(k)

	lockDirAcquired, lockFile, nonce, err := s.AcquireSessionLock(lockDir)
	if err != nil {
		return err
	}
	defer ReleaseSessionLock(lockDirAcquired, lockFile, nonce)

	// Finding #11: reject symlinks before MkdirAll to prevent symlink attacks.
	// Check all path components from root to the session file path (including leaf).
	if err := rejectSymlinksInPath(s.root, sessionPath); err != nil {
		return fmt.Errorf("commit %s: %w", k.String(), err)
	}
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(sessionPath), 0o700); err != nil {
		return fmt.Errorf("commit %s: mkdir: %w", k.String(), err)
	}
	// Post-MkdirAll symlink check: if the leaf was a symlink before MkdirAll,
	// MkdirAll may have followed it (creating dirs in the symlink target).
	// The symlink itself remains — detect it now.
	if info, err := os.Lstat(sessionPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("commit %s: symlink at session path", k.String())
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("commit %s: stat %s: %w", k.String(), sessionPath, err)
	}
	// Finding #11: tighten permissions on agent subdirectory.
	if err := validateManagedPath(s.root, filepath.Dir(sessionPath), false); err != nil {
		return fmt.Errorf("commit %s: %w", k.String(), err)
	}

	return s.writeSnapshot(sessionPath, snap)
}

// Update atomically reads the old snapshot, calls mutate, and writes the result
// under a per-session lock. The mutate function receives the old snapshot (nil if
// none exists) and returns a ReduceResult and an error. If ReduceResult.Next is nil,
// the session is deleted.
//
// If the existing snapshot is corrupt, it is quarantined and mutate receives nil
// (revision starts at 1). If mutate returns an error, the persistent state is
// unchanged and the error is returned.
func (s *Store) Update(_ context.Context, k domain.SessionKey, mutate func(old *domain.SessionSnapshot) (domain.ReduceResult, error)) error {
	if err := k.Validate(); err != nil {
		return err
	}

	lockDir := s.LockPath(k)
	sessionPath := s.Path(k)

	lockDirAcquired, lockFile, nonce, err := s.AcquireSessionLock(lockDir)
	if err != nil {
		return err
	}
	defer ReleaseSessionLock(lockDirAcquired, lockFile, nonce)

	// Finding #11: reject symlinks before MkdirAll to prevent symlink attacks.
	if err := rejectSymlinksInPath(s.root, sessionPath); err != nil {
		return fmt.Errorf("update %s: %w", k.String(), err)
	}
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(sessionPath), 0o700); err != nil {
		return fmt.Errorf("update %s: mkdir: %w", k.String(), err)
	}
	// Post-MkdirAll symlink check.
	if info, err := os.Lstat(sessionPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("update %s: symlink at session path", k.String())
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("update %s: stat %s: %w", k.String(), sessionPath, err)
	}

	// Load existing snapshot (if any).
	var old *domain.SessionSnapshot
	data, readErr := os.ReadFile(sessionPath)
	if readErr == nil {
		var snap domain.SessionSnapshot
		if jsonErr := json.Unmarshal(data, &snap); jsonErr == nil {
			if validErr := snap.Validate(); validErr == nil {
				if idErr := domain.VerifyKeySnapshotIdentity(k, &snap); idErr == nil {
					old = &snap
				} else {
					// Identity mismatch — treat as corrupt.
					readErr = fmt.Errorf("%w: %s", domain.ErrCorruptState, idErr)
				}
			} else {
				readErr = fmt.Errorf("%w: %v", domain.ErrCorruptState, validErr)
			}
		} else {
			readErr = fmt.Errorf("%w: %v", domain.ErrCorruptState, jsonErr)
		}
	} else if !os.IsNotExist(readErr) {
		return fmt.Errorf("update %s: load: %w", k.String(), readErr)
	}

	// Finding #8: quarantine corrupt snapshot before mutating.
	if readErr != nil && !os.IsNotExist(readErr) {
		if qErr := s.quarantineCorrupt(sessionPath); qErr != nil {
			return fmt.Errorf("update %s: quarantine: %w", k.String(), qErr)
		}
		old = nil
	}

	// Call mutate to compute the new snapshot. This executes INSIDE the session lock.
	// If mutate returns an error, persistent state is unchanged.
	result, err := mutate(old)
	if err != nil {
		return fmt.Errorf("update %s: reducer: %w", k.String(), err)
	}

	// Handle deletion.
	if result.Next == nil {
		// Delete the session file (if it exists).
		_ = os.Remove(sessionPath)
		return nil
	}

	// Validate the result.
	if err := result.Next.Validate(); err != nil {
		return fmt.Errorf("update %s: validate result: %w", k.String(), err)
	}
	if err := domain.VerifyKeySnapshotIdentity(k, result.Next); err != nil {
		return fmt.Errorf("update %s: identity mismatch in result: %w", k.String(), err)
	}

	// Finding #11: tighten permissions on agent subdirectory.
	if err := validateManagedPath(s.root, filepath.Dir(sessionPath), false); err != nil {
		return fmt.Errorf("update %s: %w", k.String(), err)
	}

	return s.writeSnapshot(sessionPath, *result.Next)
}

// quarantineCorrupt renames a corrupt session file to a .corrupt.<timestamp> name
// in the same directory, keeping at most 3 corrupt copies.
func (s *Store) quarantineCorrupt(sessionPath string) error {
	dir := filepath.Dir(sessionPath)
	base := filepath.Base(sessionPath)

	// List existing corrupt files for this session.
	pattern := base + ".corrupt."
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var corruptFiles []string
	for _, e := range entries {
		name := e.Name()
		if len(name) > len(base)+9 && name[:len(base)+9] == pattern {
			corruptFiles = append(corruptFiles, filepath.Join(dir, name))
		}
	}

	// If we already have 3, remove the oldest (lexicographically = by timestamp).
	if len(corruptFiles) >= 3 {
		// Sort to find the oldest; corrupt files are named with unix nano timestamps.
		oldest := corruptFiles[0]
		for _, f := range corruptFiles[1:] {
			if f < oldest {
				oldest = f
			}
		}
		if err := os.Remove(oldest); err != nil {
			return fmt.Errorf("remove corrupt %s: %w", oldest, err)
		}
	}

	// Rename current corrupt file.
	corruptPath := sessionPath + ".corrupt." + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.Rename(sessionPath, corruptPath); err != nil {
		// If rename fails (e.g., already quarantined), that's OK.
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("quarantine rename: %w", err)
		}
	}
	return nil
}

// Delete removes the session snapshot file under the session lock.
func (s *Store) Delete(_ context.Context, k domain.SessionKey) error {
	if err := k.Validate(); err != nil {
		return err
	}
	path := s.Path(k)
	lockDir := s.LockPath(k)

	lockDirAcquired, lockFile, nonce, err := s.AcquireSessionLock(lockDir)
	if err != nil {
		return err
	}
	defer ReleaseSessionLock(lockDirAcquired, lockFile, nonce)

	// Finding #11: validate managed path.
	if err := validateManagedPath(s.root, filepath.Dir(path), false); err != nil {
		return fmt.Errorf("delete %s: %w", k.String(), err)
	}

	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete %s: %w", k.String(), err)
	}
	return nil
}

// List returns all valid snapshots whose ExpiresAt is after before,
// plus a count of degraded (corrupt/invalid) entries encountered.
func (s *Store) List(_ context.Context, before time.Time) (ListResult, error) {
	base := filepath.Join(s.root, "sessions")
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return ListResult{}, nil
		}
		return ListResult{}, fmt.Errorf("list sessions dir: %w", err)
	}

	result := ListResult{}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		agent := entry.Name()
		agentDir := filepath.Join(base, agent)
		files, err := os.ReadDir(agentDir)
		if err != nil {
			// I/O error reading agent dir: count all files in this agent dir as degraded.
			// We can't distinguish file vs dir without reading, so skip and count.
			continue
		}
		for _, f := range files {
			if filepath.Ext(f.Name()) != ".json" {
				continue
			}
			filePath := filepath.Join(agentDir, f.Name())
			// Verify the file path matches the expected agent/session identity.
			// Path format: sessions/<agent>/<hash>.json
			expectedAgent := agent
			expectedHash := f.Name()[:len(f.Name())-5] // strip .json

			data, err := os.ReadFile(filePath)
			if err != nil {
				result.DegradedCount++
				continue
			}
			var snap domain.SessionSnapshot
			if err := json.Unmarshal(data, &snap); err != nil {
				result.DegradedCount++
				continue
			}
			if err := snap.Validate(); err != nil {
				result.DegradedCount++
				continue
			}
			// Verify file path identity matches snapshot.
			if snap.Agent != expectedAgent || snap.SessionIDHash != expectedHash {
				result.DegradedCount++
				continue
			}
			if snap.ExpiresAt.After(before) {
				result.Snapshots = append(result.Snapshots, snap)
			}
		}
	}
	return result, nil
}
