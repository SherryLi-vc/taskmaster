package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
	"unicode/utf8"
)

const (
	maxSnapshotSessionIDLen = 256
	maxSnapshotAgentLen     = 32
	maxSnapshotTitleLen     = 128
	maxSnapshotMessageLen   = 512
	maxSnapshotProjectLen   = 128
	maxSnapshotCwdLen       = 4096
)

// SessionSnapshot is the persisted state of a single agent session.
type SessionSnapshot struct {
	SchemaVersion        int        `json:"schema_version"`
	Revision             int        `json:"revision"`
	Agent                string     `json:"agent"`
	SessionIDHash        string     `json:"session_id_hash"`
	SessionID            string     `json:"session_id"`
	Status               Status     `json:"status"`
	Project              string     `json:"project,omitempty"`
	CWD                  string     `json:"cwd,omitempty"`
	Title                string     `json:"title,omitempty"`
	Message              string     `json:"message,omitempty"`
	PID                  int        `json:"pid,omitempty"`
	StartedAt            time.Time  `json:"started_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	ExpiresAt            time.Time  `json:"expires_at"`
	LastEventID          string     `json:"last_event_id"`
	LastErrorFingerprint string     `json:"last_error_fingerprint,omitempty"`
	Source               Source     `json:"source"`
	Capability           Capability `json:"capability"`
}

// Transition describes what changed when an event was reduced.
type Transition struct {
	From               *Status `json:"from"`
	To                 *Status `json:"to"`
	StateChanged       bool    `json:"state_changed"`
	ErrorChanged       bool    `json:"error_changed"`
	ShouldDelete       bool    `json:"should_delete"`
	IgnoredAsDuplicate bool    `json:"ignored_as_duplicate"`
}

// ReduceResult is the output of the reducer.
type ReduceResult struct {
	Next       *SessionSnapshot `json:"next"`
	Transition Transition       `json:"transition"`
}

// ErrorFingerprint computes a SHA-256 hash of the sanitized message.
type ErrorFingerprint string

func ErrorFingerprintFromMessage(msg string) ErrorFingerprint {
	sanitized := SanitizeMessage(msg)
	h := sha256.Sum256([]byte(sanitized))
	return ErrorFingerprint(hex.EncodeToString(h[:]))
}

// SessionHash computes the SHA-256 hash of the session ID string.
func SessionHash(sessionID string) string {
	h := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(h[:])
}

// CopySessionSnapshot returns a shallow copy of the snapshot.
func CopySessionSnapshot(s *SessionSnapshot) *SessionSnapshot {
	if s == nil {
		return nil
	}
	cp := *s
	return &cp
}

// ValidateSnapshot checks that the snapshot conforms to the protocol contract.
func (s SessionSnapshot) Validate() error {
	if s.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: schema_version must be %d, got %d", ErrInvalidInput, SchemaVersion, s.SchemaVersion)
	}
	if s.Revision < 1 {
		return fmt.Errorf("%w: revision must be >= 1, got %d", ErrInvalidInput, s.Revision)
	}
	if utf8.RuneCountInString(s.Agent) < 1 || utf8.RuneCountInString(s.Agent) > maxSnapshotAgentLen {
		return fmt.Errorf("%w: agent length must be 1-%d runes", ErrInvalidInput, maxSnapshotAgentLen)
	}
	if utf8.RuneCountInString(s.SessionID) < 1 || utf8.RuneCountInString(s.SessionID) > maxSnapshotSessionIDLen {
		return fmt.Errorf("%w: session_id length must be 1-%d runes", ErrInvalidInput, maxSnapshotSessionIDLen)
	}
	if !isValidStatus(s.Status) {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidInput, s.Status)
	}
	if utf8.RuneCountInString(s.Project) > maxSnapshotProjectLen {
		return fmt.Errorf("%w: project exceeds max length of %d runes", ErrInvalidInput, maxSnapshotProjectLen)
	}
	if utf8.RuneCountInString(s.CWD) > maxSnapshotCwdLen {
		return fmt.Errorf("%w: cwd exceeds max length of %d runes", ErrInvalidInput, maxSnapshotCwdLen)
	}
	if utf8.RuneCountInString(s.Title) > maxSnapshotTitleLen {
		return fmt.Errorf("%w: title exceeds max length of %d runes", ErrInvalidInput, maxSnapshotTitleLen)
	}
	if utf8.RuneCountInString(s.Message) > maxSnapshotMessageLen {
		return fmt.Errorf("%w: message exceeds max length of %d runes", ErrInvalidInput, maxSnapshotMessageLen)
	}
	if s.PID != 0 && s.PID < 1 {
		return fmt.Errorf("%w: pid must be positive when present, got %d", ErrInvalidInput, s.PID)
	}
	if s.StartedAt.IsZero() {
		return fmt.Errorf("%w: started_at is zero", ErrInvalidInput)
	}
	if s.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: updated_at is zero", ErrInvalidInput)
	}
	if s.ExpiresAt.IsZero() {
		return fmt.Errorf("%w: expires_at is zero", ErrInvalidInput)
	}
	if utf8.RuneCountInString(s.LastEventID) < 1 || utf8.RuneCountInString(s.LastEventID) > 128 {
		return fmt.Errorf("%w: last_event_id length must be 1-128 runes", ErrInvalidInput)
	}
	if s.LastErrorFingerprint != "" {
		if len(s.LastErrorFingerprint) != 64 {
			return fmt.Errorf("%w: last_error_fingerprint must be 64 hex chars", ErrInvalidInput)
		}
	}
	if !isValidSource(s.Source) {
		return fmt.Errorf("%w: unknown source %q", ErrInvalidInput, s.Source)
	}
	if !isValidCapability(s.Capability) {
		return fmt.Errorf("%w: unknown capability %q", ErrInvalidInput, s.Capability)
	}
	return nil
}

func isValidStatus(s Status) bool {
	switch s {
	case StatusWorking, StatusWaitingInput, StatusCompleted, StatusError:
		return true
	}
	return false
}
