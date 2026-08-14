//go:build windows

package store

import (
	"unsafe"

	"golang.org/x/sys/windows"
)

// replaceExistingImpl uses MoveFileEx with MOVEFILE_REPLACE_EXISTING.
// This is the sole use of golang.org/x/sys/windows in the project.
func replaceExistingImpl(newPath, oldPath string) error {
	newUTF16, err := windows.UTF16PtrFromString(newPath)
	if err != nil {
		return err
	}
	oldUTF16, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		return err
	}
	// MoveFileExW(lpExistingFileName, lpNewFileName, dwFlags)
	// lpExistingFileName = newPath (the temp file to move)
	// lpNewFileName = oldPath (the target to replace)
	// MOVEFILE_REPLACE_EXISTING = 0x1
	r, _, e := windows.NewLazySystemDLL("kernel32.dll").NewProc("MoveFileExW").Call(
		uintptr(unsafe.Pointer(newUTF16)),
		uintptr(unsafe.Pointer(oldUTF16)),
		0x1, // MOVEFILE_REPLACE_EXISTING
	)
	if r == 0 {
		return e
	}
	return nil
}
