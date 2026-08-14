package store_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/taskmaster-dev/taskmaster/internal/store"
)

// Finding #7: Platform default paths are exact.

func TestStateDirDarwin(t *testing.T) {
	r := store.PlatformResolver{
		OS:          "darwin",
		UserHomeDir: func() (string, error) { return "/Users/testuser", nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	want := filepath.Join("/Users/testuser", "Library", "Application Support", "taskmaster")
	if got != want {
		t.Errorf("StateDir() = %q, want %q", got, want)
	}
}

func TestStateDirLinuxXDG(t *testing.T) {
	r := store.PlatformResolver{
		OS: "linux",
		Getenv: func(key string) string {
			if key == "XDG_STATE_HOME" {
				return "/home/testuser/.local/state"
			}
			return ""
		},
		UserHomeDir: func() (string, error) { return "/home/testuser", nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	want := filepath.Join("/home/testuser/.local/state", "taskmaster")
	if got != want {
		t.Errorf("StateDir() = %q, want %q", got, want)
	}
}

func TestStateDirLinuxFallback(t *testing.T) {
	r := store.PlatformResolver{
		OS:          "linux",
		Getenv:      func(key string) string { return "" },
		UserHomeDir: func() (string, error) { return "/home/testuser", nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	want := filepath.Join("/home/testuser", ".local", "state", "taskmaster")
	if got != want {
		t.Errorf("StateDir() = %q, want %q", got, want)
	}
}

func TestStateDirWindows(t *testing.T) {
	r := store.PlatformResolver{
		OS: "windows",
		Getenv: func(key string) string {
			if key == "LOCALAPPDATA" {
				return `C:\Users\testuser\AppData\Local`
			}
			return ""
		},
		UserHomeDir: func() (string, error) { return `C:\Users\testuser`, nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	// On non-Windows hosts, filepath.Join uses /; accept either separator.
	want1 := `C:\Users\testuser\AppData\Local\TaskMaster`
	want2 := `C:\Users\testuser\AppData\Local/TaskMaster`
	if got != want1 && got != want2 {
		t.Errorf("StateDir() = %q, want one of %q or %q", got, want1, want2)
	}
}

func TestStateDirWindowsFallback(t *testing.T) {
	// LOCALAPPDATA not set; falls back to home-based path.
	r := store.PlatformResolver{
		OS:          "windows",
		Getenv:      func(key string) string { return "" },
		UserHomeDir: func() (string, error) { return `C:\Users\testuser`, nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	// Use filepath.Join so separator matches current platform.
	want := filepath.Join(`C:\Users\testuser`, "AppData", "Local", "TaskMaster")
	if got != want {
		t.Errorf("StateDir() = %q, want %q", got, want)
	}
}

func TestOpenDefaultCreatesStore(t *testing.T) {
	// Only run this test on the current platform.
	resolver := store.PlatformResolver{
		OS: runtime.GOOS,
		Getenv: func(key string) string {
			if key == "XDG_STATE_HOME" {
				return t.TempDir()
			}
			return os.Getenv(key)
		},
		UserHomeDir: func() (string, error) { return t.TempDir(), nil },
	}
	dir, err := resolver.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	s, err := store.New(dir)
	if err != nil {
		t.Fatalf("store.New() error = %v", err)
	}
	if s.Root() != dir {
		t.Errorf("Root() = %q, want %q", s.Root(), dir)
	}
}

func TestStateDirUnknownPlatform(t *testing.T) {
	r := store.PlatformResolver{
		OS:          "plan9",
		UserHomeDir: func() (string, error) { return "/home/testuser", nil },
	}
	got, err := r.StateDir()
	if err != nil {
		t.Fatalf("StateDir() error = %v", err)
	}
	want := filepath.Join("/home/testuser", ".local", "state", "taskmaster")
	if got != want {
		t.Errorf("StateDir() = %q, want %q", got, want)
	}
}
