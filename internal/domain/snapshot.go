package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

var sessionKeyAgentPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

const (
	maxSessionKeyAgentLen = 32
	maxSessionKeyHashLen  = 64
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
		if !isLowerHex(s.LastErrorFingerprint) {
			return fmt.Errorf("%w: last_error_fingerprint must be lowercase hex", ErrInvalidInput)
		}
	}
	if !isValidSource(s.Source) {
		return fmt.Errorf("%w: unknown source %q", ErrInvalidInput, s.Source)
	}
	if !isValidCapability(s.Capability) {
		return fmt.Errorf("%w: unknown capability %q", ErrInvalidInput, s.Capability)
	}
	if s.SessionIDHash != SessionHash(s.SessionID) {
		return fmt.Errorf("%w: session_id_hash does not match session_id", ErrInvalidInput)
	}
	return nil
}

// VerifyKeySnapshotIdentity returns nil if the key's agent, session_id, and hash
// all match the snapshot's corresponding fields. This prevents a valid key from
// being used to read or write a snapshot belonging to a different session.
func VerifyKeySnapshotIdentity(k SessionKey, snap *SessionSnapshot) error {
	if snap == nil {
		return fmt.Errorf("%w: snapshot is nil", ErrInvalidInput)
	}
	if k.Agent != snap.Agent {
		return fmt.Errorf("%w: key agent %q does not match snapshot agent %q", ErrInvalidInput, k.Agent, snap.Agent)
	}
	if k.SessionID != snap.SessionID {
		return fmt.Errorf("%w: key session_id does not match snapshot session_id", ErrInvalidInput)
	}
	if k.SessionIDH != snap.SessionIDHash {
		return fmt.Errorf("%w: key session_id_hash does not match snapshot session_id_hash", ErrInvalidInput)
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

func isLowerHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return len(s) > 0
}

// SessionKey uniquely identifies a persisted session snapshot.
type SessionKey struct {
	Agent      string
	SessionID  string
	SessionIDH string
}

// String returns the agent/hash path component: "<agent>/<sha256>".
func (k SessionKey) String() string {
	return k.Agent + "/" + k.SessionIDH
}

// NewSessionKey creates a SessionKey from agent and sessionID, computing the SHA-256 hash.
func NewSessionKey(agent, sessionID string) SessionKey {
	return SessionKey{
		Agent:      agent,
		SessionID:  sessionID,
		SessionIDH: SessionHash(sessionID),
	}
}

// Validate checks that the key conforms to the store contract.
func (k SessionKey) Validate() error {
	if utf8.RuneCountInString(k.Agent) < 1 || utf8.RuneCountInString(k.Agent) > maxSessionKeyAgentLen {
		return fmt.Errorf("%w: agent length must be 1-%d runes", ErrInvalidInput, maxSessionKeyAgentLen)
	}
	if !sessionKeyAgentPattern.MatchString(k.Agent) {
		return fmt.Errorf("%w: agent has invalid characters", ErrInvalidInput)
	}
	if utf8.RuneCountInString(k.SessionID) < 1 {
		return fmt.Errorf("%w: session_id must be non-empty", ErrInvalidInput)
	}
	if len(k.SessionIDH) != maxSessionKeyHashLen {
		return fmt.Errorf("%w: session_id_hash must be %d hex chars, got %d", ErrInvalidInput, maxSessionKeyHashLen, len(k.SessionIDH))
	}
	if !isLowerHex(k.SessionIDH) {
		return fmt.Errorf("%w: session_id_hash must be lowercase hex", ErrInvalidInput)
	}
	if k.SessionIDH != SessionHash(k.SessionID) {
		return fmt.Errorf("%w: session_id_hash does not match session_id", ErrInvalidInput)
	}
	return nil
}
