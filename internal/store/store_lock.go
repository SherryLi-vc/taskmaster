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
	tmp, err := createTempSeam(filepath.Dir(sessionPath), ".tmp-*.json")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		// Clean up temp file if it still exists.
		removeSeam(tmpPath)
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

// -- Fault injection seams (P1-4) --
// These package-private function variables allow tests to inject failures at
// each stage of the atomic write protocol. Tests in store_test can override
// them via the exported Set*/Reset* helpers below.

var (
	createTempSeam      = os.CreateTemp
	replaceExistingSeam = replaceExisting
	removeSeam          = os.Remove
	removeAllSeam       = os.RemoveAll
	syncParentDirSeam   = syncParentDir
)

// SetCreateTempSeam overrides the temp file creation function for testing.
func SetCreateTempSeam(fn func(dir, pattern string) (*os.File, error)) {
	createTempSeam = fn
}

// ResetCreateTempSeam restores the default temp file creation function.
func ResetCreateTempSeam() {
	createTempSeam = os.CreateTemp
}

// SetReplaceExistingSeam overrides the atomic replace function for testing.
func SetReplaceExistingSeam(fn func(newPath, oldPath string) error) {
	replaceExistingSeam = fn
}

// ResetReplaceExistingSeam restores the default atomic replace function.
func ResetReplaceExistingSeam() {
	replaceExistingSeam = replaceExisting
}

// SetRemoveSeam overrides the remove function for testing.
func SetRemoveSeam(fn func(name string) error) {
	removeSeam = fn
}

// ResetRemoveSeam restores the default remove function.
func ResetRemoveSeam() {
	removeSeam = os.Remove
}

// SetRemoveAllSeam overrides the remove-all function for testing.
func SetRemoveAllSeam(fn func(path string) error) {
	removeAllSeam = fn
}

// ResetRemoveAllSeam restores the default remove-all function.
func ResetRemoveAllSeam() {
	removeAllSeam = os.RemoveAll
}

// SetSyncParentDirSeam overrides the parent-dir sync function for testing.
func SetSyncParentDirSeam(fn func(path string) error) {
	syncParentDirSeam = fn
}

// ResetSyncParentDirSeam restores the default parent-dir sync function.
func ResetSyncParentDirSeam() {
	syncParentDirSeam = syncParentDir
}

// -- Fault injection seams for file operations (P1-4) --

var (
	writeFileSeam = func(f *os.File, b []byte) (int, error) { return f.Write(b) }
	syncFileSeam  = func(f *os.File) error { return f.Sync() }
	chmodFileSeam = func(f *os.File, mode os.FileMode) error { return f.Chmod(mode) }
	closeFileSeam = func(f *os.File) error { return f.Close() }
)

// SetWriteFileSeam overrides the file write function for testing.
func SetWriteFileSeam(fn func(f *os.File, b []byte) (int, error)) {
	writeFileSeam = fn
}

// ResetWriteFileSeam restores the default file write function.
func ResetWriteFileSeam() {
	writeFileSeam = func(f *os.File, b []byte) (int, error) { return f.Write(b) }
}

// SetSyncFileSeam overrides the file sync function for testing.
func SetSyncFileSeam(fn func(f *os.File) error) {
	syncFileSeam = fn
}

// ResetSyncFileSeam restores the default file sync function.
func ResetSyncFileSeam() {
	syncFileSeam = func(f *os.File) error { return f.Sync() }
}

// SetChmodFileSeam overrides the file chmod function for testing.
func SetChmodFileSeam(fn func(f *os.File, mode os.FileMode) error) {
	chmodFileSeam = fn
}

// ResetChmodFileSeam restores the default file chmod function.
func ResetChmodFileSeam() {
	chmodFileSeam = func(f *os.File, mode os.FileMode) error { return f.Chmod(mode) }
}

// SetCloseFileSeam overrides the file close function for testing.
func SetCloseFileSeam(fn func(f *os.File) error) {
	closeFileSeam = fn
}

// ResetCloseFileSeam restores the default file close function.
func ResetCloseFileSeam() {
	closeFileSeam = func(f *os.File) error { return f.Close() }
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
				// P1-8: Stat failure must respect the 75ms deadline budget.
				// Only ENOENT is a transient race worth retrying; other errors abort.
				if time.Now().After(deadline) {
					return "", "", "", fmt.Errorf("%w: lock timeout", domain.ErrLockTimeout)
				}
				if errors.Is(statErr, os.ErrNotExist) {
					// Transient race: directory disappeared between Mkdir and Stat.
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
// It returns an error only when deletion fails; safety skips (nonce mismatch,
// unable to verify ownership) return nil because we chose not to delete.
func ReleaseSessionLock(lockDir, lockFile, nonce string) error {
	if lockDir == "" {
		return nil // never acquired
	}
	// Verify ownership before releasing: only delete if nonce was successfully
	// read AND fully matches.
	if nonce != "" {
		data, readErr := os.ReadFile(lockFile)
		if readErr == nil {
			// nonce is stored as "<hex>\n"
			parts := splitNonce(string(data))
			if parts != nonce {
				return nil // not our lock anymore
			}
		} else {
			return nil // cannot verify ownership; do not delete
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
