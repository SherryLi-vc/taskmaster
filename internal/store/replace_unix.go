//go:build !windows

package store

import "os"

// replaceExistingImpl uses os.Rename for atomic replacement.
// On Unix, Rename atomically replaces the destination if it exists.
func replaceExistingImpl(newPath, oldPath string) error {
	return os.Rename(newPath, oldPath)
}
