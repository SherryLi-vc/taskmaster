package store

import (
	"os"
	"path/filepath"
	"runtime"
)

// PlatformResolver provides OS, home, and environment lookups for path resolution.
// Inject these in tests to avoid modifying real HOME or reading real env vars.
type PlatformResolver struct {
	OS          string // GOOS value; empty string means use runtime.GOOS
	Home        func() (string, error)
	Getenv      func(key string) string
	UserHomeDir func() (string, error)
}

// defaultResolver returns a resolver wired to the real OS.
func defaultResolver() PlatformResolver {
	return PlatformResolver{
		OS:          runtime.GOOS,
		Home:        os.UserHomeDir,
		Getenv:      os.Getenv,
		UserHomeDir: os.UserHomeDir,
	}
}

// DefaultStateDir returns the platform-appropriate default directory for
// TaskMaster state:
//   - macOS:   ~/Library/Application Support/taskmaster/
//   - Linux:   $XDG_STATE_HOME/taskmaster/ or ~/.local/state/taskmaster/
//   - Windows: %LOCALAPPDATA%\TaskMaster\
func DefaultStateDir() (string, error) {
	return defaultResolver().StateDir()
}

// StateDir returns the platform state directory using this resolver.
func (r PlatformResolver) StateDir() (string, error) {
	goos := r.OS
	if goos == "" {
		goos = runtime.GOOS
	}
	if r.Getenv == nil {
		r.Getenv = os.Getenv
	}
	if r.UserHomeDir == nil {
		r.UserHomeDir = os.UserHomeDir
	}

	switch goos {
	case "darwin":
		home, err := r.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, "Library", "Application Support", "taskmaster"), nil
	case "linux":
		if dir := r.Getenv("XDG_STATE_HOME"); dir != "" {
			return filepath.Join(dir, "taskmaster"), nil
		}
		home, err := r.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local", "state", "taskmaster"), nil
	case "windows":
		localAppData := r.Getenv("LOCALAPPDATA")
		if localAppData == "" {
			// Fallback: try home directory
			home, err := r.UserHomeDir()
			if err != nil {
				return "", err
			}
			localAppData = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(localAppData, "TaskMaster"), nil
	default:
		// Unknown platform: fall back to XDG-like path
		home, err := r.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local", "state", "taskmaster"), nil
	}
}

// OpenDefault creates or opens a Store at the platform default state directory.
func OpenDefault() (*Store, error) {
	dir, err := DefaultStateDir()
	if err != nil {
		return nil, err
	}
	return New(dir)
}
