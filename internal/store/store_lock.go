package store

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
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
	tmp, err := createTempSeam(filepath.Dir(sessionPath), ".tmp-*.json")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	// Track temp cleanup error separately; it takes priority over subsequent
	// errors only if no later step fails. This ensures temp file removal errors
	// are not silently swallowed by the parent sync that follows.
	var tempRemoveErr error
	defer func() {
		// Clean up temp file if it still exists.
		if rmErr := removeSeam(tmpPath); rmErr != nil && !os.IsNotExist(rmErr) {
			tempRemoveErr = rmErr
		}
	}()

	// Write JSON data.
	if _, err := writeFileSeam(tmp, data); err != nil {
		closeFileSeam(tmp)
		return fmt.Errorf("write temp: %w", err)
	}

	// fsync file data.
	if err := syncFileSeam(tmp); err != nil {
		closeFileSeam(tmp)
		return fmt.Errorf("sync temp: %w", err)
	}

	// Chmod to 0600 (Unix only; Chmod on Windows may be a no-op).
	if runtime.GOOS != "windows" {
		if err := chmodFileSeam(tmp, 0o600); err != nil {
			closeFileSeam(tmp)
			return fmt.Errorf("chmod temp: %w", err)
		}
	}

	// Close the file before rename.
	if err := closeFileSeam(tmp); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}

	// Atomic rename to target.
	if err := replaceExistingSeam(tmpPath, sessionPath); err != nil {
		return fmt.Errorf("rename: %w", err)
	}

	// fsync parent directory. On Unix, directory fsync is supported and errors
	// are returned. On Windows, this is a no-op because Windows does not
	// expose directory fsync via os.Open/Sync; rename itself provides NTFS durability.
	if err := syncParentDirSeam(sessionPath); err != nil {
		return fmt.Errorf("sync parent: %w", err)
	}

	// Return temp file removal error if one occurred (after rename).
	if tempRemoveErr != nil {
		return fmt.Errorf("remove temp: %w", tempRemoveErr)
	}

	return nil
}

// syncParentDir fsyncs the parent directory of path on platforms that support
// directory-level fsync. On Unix (Linux, macOS) it opens the parent directory,
// calls Sync, and closes it, returning any error. On Windows it returns nil
// because Windows does not support directory fsync through this interface;
// the atomic rename itself provides durability on NTFS.
func syncParentDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	dir := filepath.Dir(path)
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open parent dir %s: %w", dir, err)
	}
	if syncErr := d.Sync(); syncErr != nil {
		d.Close()
		return fmt.Errorf("sync parent dir %s: %w", dir, syncErr)
	}
	if closeErr := d.Close(); closeErr != nil {
		return fmt.Errorf("close parent dir %s: %w", dir, closeErr)
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

// -- Fault injection seams (P1-4, issue #8) --
// These are unexported package-level variables. Tests in package store can
// override them directly and restore via t.Cleanup or defer.
// DO NOT export Set*/Reset* helpers; they are not part of the public API.

var (
	createTempSeam      = os.CreateTemp
	replaceExistingSeam = replaceExisting
	removeSeam          = os.Remove
	removeAllSeam       = os.RemoveAll
	syncParentDirSeam   = syncParentDir
	readFileSeam        = os.ReadFile
)

// fileOpSeams are per-operation overrides for the atomic write protocol.
// Each seam returns the default implementation; tests assign custom functions.
var (
	writeFileSeam = func(f *os.File, b []byte) (int, error) { return f.Write(b) }
	syncFileSeam  = func(f *os.File) error { return f.Sync() }
	chmodFileSeam = func(f *os.File, mode os.FileMode) error { return f.Chmod(mode) }
	closeFileSeam = func(f *os.File) error { return f.Close() }
)

// -- Lock implementation (finding #2, #3) --

const (
	maxLockJitterMs = 75 * time.Millisecond
	staleLock       = 2 * time.Second
	lockRetryDelay  = 5 * time.Millisecond
)

// isTransientWindowsStatError classifies a Stat error on Windows as transient
// (retryable) or permanent. Only specific contention errno values are transient:
//   - ERROR_ACCESS_DENIED (syscall errno 5): another process holds a file handle open
//   - ERROR_SHARING_VIOLATION (syscall errno 32): file is locked by another process
//
// All other errors (path errors, permission errors, etc.) are permanent and
// must be propagated immediately. On non-Windows platforms, only ENOENT is
// transient (handled separately); everything else is permanent.
func isTransientWindowsStatError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrNotExist) {
		return true // ENOENT is always transient (race between Mkdir and Stat)
	}
	if runtime.GOOS != "windows" {
		return false // On Unix, non-ENOENT errors are permanent
	}
	// On Windows, check for specific transient contention errno values.
	// Use raw values to avoid build failures on non-Windows platforms where
	// these named constants are not defined.
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case syscall.Errno(5), syscall.Errno(32): // ERROR_ACCESS_DENIED, ERROR_SHARING_VIOLATION
			return true
		}
	}
	return false
}

// isWindowsMkdirAccessDenied returns true when a Mkdir error on Windows should
// be treated as EEXIST (directory exists but is temporarily inaccessible).
// Uses raw errno value to avoid build failures on non-Windows platforms.
func isWindowsMkdirAccessDenied(err error) bool {
	if runtime.GOOS != "windows" {
		return false
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.Errno(5) // ERROR_ACCESS_DENIED
	}
	return false
}

// min returns the smaller of two time.Duration values.
func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// AcquireSessionLock acquires an exclusive per-session lock using mkdir.
// It returns the lock directory path, the nonce file path, the nonce value, and error.
// If the lock cannot be acquired within the jitter budget, it returns ErrLockTimeout.
// P1-7/P1-8: every retry path is bounded by the 75ms deadline; sleep is capped
// to remaining budget; non-ENOENT Stat errors abort immediately.
func (s *Store) AcquireSessionLock(lockDir string) (string, string, string, error) {
	nonce, err := randNonce()
	if err != nil {
		return "", "", "", fmt.Errorf("nonce: %w", err)
	}

	deadline := time.Now().Add(maxLockJitterMs)
	lockFile := filepath.Join(lockDir, ".lock")

	for {
		// P1-8: check deadline at the TOP of every iteration, before Mkdir.
		if time.Now().After(deadline) {
			return "", "", "", fmt.Errorf("%w: lock timeout", domain.ErrLockTimeout)
		}

		// Create lock dir exclusively. Mkdir is atomic; EEXIST means someone else holds it.
		if err := os.Mkdir(lockDir, 0o700); err != nil {
			if !errors.Is(err, os.ErrExist) {
				// Windows may return ERROR_ACCESS_DENIED when the directory already
				// exists but is temporarily inaccessible. Treat as EEXIST if the
				// directory actually exists.
				if isWindowsMkdirAccessDenied(err) {
					if _, statErr := os.Stat(lockDir); statErr == nil {
						err = os.ErrExist
					}
				}
			}
			if !errors.Is(err, os.ErrExist) {
				return "", "", "", fmt.Errorf("mkdir lock: %w", err)
			}
			// Lock dir exists — check if stale.
			info, statErr := os.Stat(lockDir)
			if statErr != nil {
				// P1-8/P1-4: Stat failure must respect the 75ms deadline budget.
				// ENOENT is transient (race); on Windows, specific contention errno
				// values are also transient (retry). All other errors are permanent.
				if time.Now().After(deadline) {
					return "", "", "", fmt.Errorf("%w: lock timeout", domain.ErrLockTimeout)
				}
				if errors.Is(statErr, os.ErrNotExist) || isTransientWindowsStatError(statErr) {
					// Transient: retry with capped sleep.
					remaining := time.Until(deadline)
					if remaining > 0 {
						time.Sleep(min(lockRetryDelay, remaining))
					}
					continue
				}
				return "", "", "", fmt.Errorf("stat lock: %w", statErr)
			}
			if time.Since(info.ModTime()) > staleLock {
				// Stale takeover: rename to a unique stale path before removing.
				stalePath := lockDir + ".stale." + fmt.Sprintf("%d", time.Now().UnixNano())
				if renameErr := os.Rename(lockDir, stalePath); renameErr == nil {
					if removeErr := removeAllSeam(stalePath); removeErr != nil {
						return "", "", "", fmt.Errorf("remove stale lock: %w", removeErr)
					}
					continue // retry
				}
				// If rename fails, the lock is still live — do NOT delete it.
				// Just fall through to sleep + retry below.
			}
			// Not stale (or stale but rename failed); check deadline and sleep.
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return "", "", "", fmt.Errorf("%w: lock timeout", domain.ErrLockTimeout)
			}
			sleepDuration := lockRetryDelay
			if remaining < sleepDuration {
				sleepDuration = remaining
			}
			time.Sleep(sleepDuration)
			continue
		}

		// Lock dir acquired — write the owner nonce.
		if err := os.WriteFile(lockFile, []byte(nonce+"\n"), 0o600); err != nil {
			removeSeam(lockDir)
			return "", "", "", fmt.Errorf("write nonce: %w", err)
		}
		// fsync the lock file.
		if f, err := os.OpenFile(lockFile, os.O_RDWR, 0o600); err == nil {
			if syncErr := f.Sync(); syncErr != nil {
				f.Close()
				removeSeam(lockFile)
				removeSeam(lockDir)
				return "", "", "", fmt.Errorf("sync nonce: %w", syncErr)
			}
			if closeErr := f.Close(); closeErr != nil {
				removeSeam(lockFile)
				removeSeam(lockDir)
				return "", "", "", fmt.Errorf("close nonce: %w", closeErr)
			}
		} else {
			removeSeam(lockFile)
			removeSeam(lockDir)
			return "", "", "", fmt.Errorf("open nonce: %w", err)
		}
		return lockDir, lockFile, nonce, nil
	}
}

// ReleaseSessionLock releases the session lock only if we still own it
// (nonce matches). This prevents a stale holder from deleting a new holder's lock.
// P2-1: returns nil only when deletion succeeds or is correctly skipped.
// Returns error for any cleanup failure or inability to verify ownership,
// so callers can observe that the persistent lock artifact may still exist.
func ReleaseSessionLock(lockDir, lockFile, nonce string) error {
	if lockDir == "" {
		return nil // never acquired, nothing to clean up
	}
	// Verify ownership before releasing: only delete if nonce was successfully
	// read AND fully matches.
	if nonce != "" {
		data, readErr := readFileSeam(lockFile)
		if readErr == nil {
			// nonce is stored as "<hex>\n"
			parts := splitNonce(string(data))
			if parts != nonce {
				return nil // not our lock anymore; another holder cleaned up
			}
		} else {
			// Cannot verify ownership — do not delete (safety).
			// But return error so caller knows cleanup state is uncertain.
			return fmt.Errorf("cannot verify lock ownership (read nonce: %w); lock not deleted", readErr)
		}
	}
	if err := removeSeam(lockFile); err != nil {
		return fmt.Errorf("remove lock file: %w", err)
	}
	if err := removeSeam(lockDir); err != nil {
		return fmt.Errorf("remove lock dir: %w", err)
	}
	return nil
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
