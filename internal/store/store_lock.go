package store

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

// Finding #6: ListResult carries healthy snapshots plus a degraded count for
// files that could not be read or validated.
type ListResult struct {
	Snapshots     []domain.SessionSnapshot
	DegradedCount int
}

// ListResultOrError is returned by List when a root-level I/O error occurs.
// Snapshots contains whatever was successfully read before the error;
// DegradedCount reflects files skipped during traversal.
type ListResultOrError struct {
	Result ListResult
	Err    error
}

// --- Private helpers ---

// randNonce generates a random 16-byte hex owner token.
func randNonce() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", buf[:]), nil
}

// isIOError returns true for filesystem I/O errors (excluding not-exist).
func isIOError(err error) bool {
	if err == nil {
		return false
	}
	return !errors.Is(err, os.ErrNotExist)
}

// validateManagedPath ensures the target path is not a symlink.
// If the target exists:
//   - For directories: mode is tightened to 0700
//   - For files: mode is tightened to 0600
//
// If the target does not exist (e.g., session not yet created), no error.
// Symlinks at the target are always rejected.
func validateManagedPath(base, target string, isFile bool) error {
	// Target may not exist yet (e.g., session file not yet created).
	info, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("managed path lstat %s: %w", target, err)
	}
	// Reject symlinks at the target.
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("managed path is symlink: %s", target)
	}
	if info.IsDir() {
		if info.Mode().Perm() != 0o700 {
			if err := os.Chmod(target, 0o700); err != nil {
				return fmt.Errorf("tighten dir %s to 0700: %w", target, err)
			}
		}
	} else if isFile {
		if info.Mode().Perm() != 0o600 {
			if err := os.Chmod(target, 0o600); err != nil {
				return fmt.Errorf("tighten file %s to 0600: %w", target, err)
			}
		}
	}
	return nil
}

// rejectSymlinksInPath checks that no path component between base and target
// is a symlink. This must be called BEFORE MkdirAll to prevent symlink
// attacks: MkdirAll would follow a symlink at any intermediate level and
// create directories in the symlink target.
//
// The check validates every component in the path chain from base to target,
// including components that are not directories (e.g., if "sessions/claude/sess-123"
// is a symlink, MkdirAll("sessions/claude/sess-123.json") follows it).
// Non-existent components are skipped (MkdirAll will create them safely).
func rejectSymlinksInPath(base, target string) error {
	rel, err := filepath.Rel(base, target)
	if err != nil {
		return err
	}

	// Walk all components of the relative path.
	// Use filepath.Split to step through: "a/b/c/d.json" → "a/b/c/" + "d.json" → "a/b/" + "c/" → ...
	cur := rel
	for cur != "." && cur != "/" {
		dir, file := filepath.Split(cur)
		if file == "" {
			break
		}
		// Check the directory portion: "a/b/c/" → check "a/b/c"
		if dir != "" {
			dir = dir[:len(dir)-1] // trim trailing separator
			fullPath := filepath.Join(base, dir)
			if info, err := os.Lstat(fullPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("symlink detected in managed path: %s", fullPath)
			}
		}
		// Check the file portion (intermediate basename): "d.json" → check "d.json"
		fullPath := filepath.Join(base, cur)
		if info, err := os.Lstat(fullPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink detected in managed path: %s", fullPath)
		}
		cur = dir
		if cur == "" {
			break
		}
	}
	return nil
}

// writeSnapshot writes snap to sessionPath atomically using a unique temp file.
// It returns an error on any failure; the old sessionPath is preserved on error.
func (s *Store) writeSnapshot(sessionPath string, snap domain.SessionSnapshot) error {
	data, err := jsonMarshalIndent(snap)
	if err != nil {
		return err
	}
	// Finding #10: snapshot JSON file must end with a newline.
	data = append(data, '\n')

	// Create a unique temp file in the same directory.
	tmp, err := os.CreateTemp(filepath.Dir(sessionPath), ".tmp-*.json")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		// Clean up temp file if it still exists.
		os.Remove(tmpPath)
	}()

	// Write JSON data.
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write temp: %w", err)
	}

	// fsync file data.
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync temp: %w", err)
	}

	// Chmod to 0600 (Unix only; Chmod on Windows may be a no-op).
	if runtime.GOOS != "windows" {
		if err := tmp.Chmod(0o600); err != nil {
			return fmt.Errorf("chmod temp: %w", err)
		}
	}

	// Close the file before rename.
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// Atomic rename to target.
	if err := replaceExisting(tmpPath, sessionPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// fsync parent directory (best-effort).
	if runtime.GOOS != "windows" {
		if d, err := os.Open(filepath.Dir(sessionPath)); err == nil {
			if syncErr := d.Sync(); syncErr != nil {
				_ = syncErr // best-effort; rename itself is durable
			}
			if closeErr := d.Close(); closeErr != nil {
				_ = closeErr // best-effort
			}
		}
		// Ignore parent open/sync/close errors; the rename itself is durable.
	}

	return nil
}

// jsonMarshalIndent is a package-private wrapper for json.MarshalIndent.
func jsonMarshalIndent(v interface{}) ([]byte, error) {
	return jsonMarshalIndentFn(v)
}

// jsonMarshalIndentFn allows tests to override JSON marshaling.
var jsonMarshalIndentFn = jsonMarshalIndentStd

func jsonMarshalIndentStd(v interface{}) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// SetJSONMarshalIndent overrides the JSON marshal function for testing.
// Call ResetJSONMarshalIndent to restore the default.
func SetJSONMarshalIndent(fn func(v interface{}) ([]byte, error)) {
	jsonMarshalIndentFn = fn
}

// ResetJSONMarshalIndent restores the default JSON marshal function.
func ResetJSONMarshalIndent() {
	jsonMarshalIndentFn = jsonMarshalIndentStd
}

// -- Lock implementation (finding #2, #3) --

const (
	maxLockJitterMs = 75 * time.Millisecond
	staleLock       = 2 * time.Second
	lockRetryDelay  = 5 * time.Millisecond
)

// acquireSessionLock acquires an exclusive per-session lock using mkdir.
// It returns the lock directory path, the nonce file path, the nonce value, and error.
// If the lock cannot be acquired within the jitter budget, it returns ErrLockTimeout.
func (s *Store) AcquireSessionLock(lockDir string) (string, string, string, error) {
	nonce, err := randNonce()
	if err != nil {
		return "", "", "", fmt.Errorf("nonce: %w", err)
	}

	deadline := time.Now().Add(maxLockJitterMs)
	lockFile := filepath.Join(lockDir, ".lock")

	for {
		// Create lock dir exclusively. Mkdir is atomic; EEXIST means someone else holds it.
		if err := os.Mkdir(lockDir, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				return "", "", "", fmt.Errorf("mkdir lock: %w", err)
			}
			// Lock dir exists — check if stale.
			info, statErr := os.Stat(lockDir)
			if statErr != nil {
				// Race condition: directory disappeared between Mkdir and Stat.
				continue
			}
			if time.Since(info.ModTime()) > staleLock {
				// Stale takeover: rename to a unique stale path before removing.
				stalePath := lockDir + ".stale." + fmt.Sprintf("%d", time.Now().UnixNano())
				if renameErr := os.Rename(lockDir, stalePath); renameErr == nil {
					if removeErr := os.RemoveAll(stalePath); removeErr != nil {
						return "", "", "", fmt.Errorf("remove stale lock: %w", removeErr)
					}
					continue // retry
				}
				// If rename fails, the lock is still live — do NOT delete it.
				// Just wait and retry.
			}
			// Not stale; check deadline before sleeping.
			if time.Now().After(deadline) {
				return "", "", "", fmt.Errorf("%w: lock timeout", domain.ErrLockTimeout)
			}
			time.Sleep(lockRetryDelay)
			continue
		}

		// Lock dir acquired — write the owner nonce.
		if err := os.WriteFile(lockFile, []byte(nonce+"\n"), 0o600); err != nil {
			os.Remove(lockDir)
			return "", "", "", fmt.Errorf("write nonce: %w", err)
		}
		// fsync the lock file.
		if f, err := os.OpenFile(lockFile, os.O_RDWR, 0o600); err == nil {
			if syncErr := f.Sync(); syncErr != nil {
				f.Close()
				os.Remove(lockFile)
				os.Remove(lockDir)
				return "", "", "", fmt.Errorf("sync nonce: %w", syncErr)
			}
			if closeErr := f.Close(); closeErr != nil {
				os.Remove(lockFile)
				os.Remove(lockDir)
				return "", "", "", fmt.Errorf("close nonce: %w", closeErr)
			}
		} else {
			os.Remove(lockFile)
			os.Remove(lockDir)
			return "", "", "", fmt.Errorf("open nonce: %w", err)
		}
		return lockDir, lockFile, nonce, nil
	}
}

// ReleaseSessionLock releases the session lock only if we still own it
// (nonce matches). This prevents a stale holder from deleting a new holder's lock.
func ReleaseSessionLock(lockDir, lockFile, nonce string) {
	if lockDir == "" {
		return // never acquired
	}
	// Verify ownership before releasing: only delete if nonce was successfully
	// read AND fully matches.
	if nonce != "" {
		data, readErr := os.ReadFile(lockFile)
		if readErr == nil {
			// nonce is stored as "<hex>\n"
			parts := splitNonce(string(data))
			if parts != nonce {
				return // not our lock anymore
			}
		} else {
			return // cannot verify ownership; do not delete
		}
	}
	os.Remove(lockFile)
	os.Remove(lockDir)
}

func splitNonce(s string) string {
	// Strip trailing newline.
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '\n' || s[i] == '\r' {
			return s[:i]
		}
	}
	return s
}

// -- replaceExisting (finding #5) --

// replaceExisting atomically replaces oldPath with newPath.
// Platform-specific implementations are in replace_unix.go and replace_windows.go.
func replaceExisting(newPath, oldPath string) error {
	return replaceExistingImpl(newPath, oldPath)
}
