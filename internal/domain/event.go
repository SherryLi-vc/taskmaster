package domain

import (
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	eventIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]+$`)
	agentPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)
)

const (
	maxEventIDLen    = 128
	maxAgentLen      = 32
	maxSessionIDLen  = 256
	maxProjectLen    = 128
	maxCwdLen        = 4096
	maxTitleLen      = 128
	maxMessageLen    = 512
	maxMetadataCount = 16
	maxMetadataValue = 256
)

// EventKind identifies the type of event.
type EventKind string

const (
	EventSessionStarted EventKind = "session_started"
	EventWorkStarted    EventKind = "work_started"
	EventProgress       EventKind = "progress"
	EventInputRequired  EventKind = "input_required"
	EventTurnCompleted  EventKind = "turn_completed"
	EventFailed         EventKind = "failed"
	EventSessionEnded   EventKind = "session_ended"
	EventCleared        EventKind = "cleared"
)

// Source identifies where the event originated.
type Source string

const (
	SourceHook   Source = "hook"
	SourceNotify Source = "notify"
	SourceManual Source = "manual"
	SourceLog    Source = "log"
)

// Capability identifies the adapter's event generation capability.
type Capability string

const (
	CapabilityFull           Capability = "full"
	CapabilityCompletionOnly Capability = "completion_only"
	CapabilityManual         Capability = "manual"
)

// Event is a normalized event from any agent adapter.
type Event struct {
	SchemaVersion int               `json:"schema_version"`
	EventID       string            `json:"event_id"`
	Agent         string            `json:"agent"`
	SessionID     string            `json:"session_id"`
	Kind          EventKind         `json:"kind"`
	OccurredAt    time.Time         `json:"occurred_at"`
	Source        Source            `json:"source"`
	Capability    Capability        `json:"capability"`
	Project       string            `json:"project,omitempty"`
	CWD           string            `json:"cwd,omitempty"`
	Title         string            `json:"title,omitempty"`
	Message       string            `json:"message,omitempty"`
	PID           int               `json:"pid,omitempty"`
	Severity      string            `json:"severity,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// Validate checks that the event conforms to the protocol contract.
func (e Event) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: schema_version must be %d, got %d", ErrInvalidInput, SchemaVersion, e.SchemaVersion)
	}
	if utf8.RuneCountInString(e.EventID) < 1 || utf8.RuneCountInString(e.EventID) > maxEventIDLen {
		return fmt.Errorf("%w: event_id length must be 1-%d runes, got %d", ErrInvalidInput, maxEventIDLen, utf8.RuneCountInString(e.EventID))
	}
	if !eventIDPattern.MatchString(e.EventID) {
		return fmt.Errorf("%w: event_id has invalid characters", ErrInvalidInput)
	}
	if utf8.RuneCountInString(e.Agent) < 1 || utf8.RuneCountInString(e.Agent) > maxAgentLen {
		return fmt.Errorf("%w: agent length must be 1-%d runes, got %d", ErrInvalidInput, maxAgentLen, utf8.RuneCountInString(e.Agent))
	}
	if !agentPattern.MatchString(e.Agent) {
		return fmt.Errorf("%w: agent has invalid characters", ErrInvalidInput)
	}
	if utf8.RuneCountInString(e.SessionID) < 1 || utf8.RuneCountInString(e.SessionID) > maxSessionIDLen {
		return fmt.Errorf("%w: session_id length must be 1-%d runes, got %d", ErrInvalidInput, maxSessionIDLen, utf8.RuneCountInString(e.SessionID))
	}
	if !isValidEventKind(e.Kind) {
		return fmt.Errorf("%w: unknown event kind %q", ErrInvalidInput, e.Kind)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("%w: occurred_at is zero", ErrInvalidInput)
	}
	// Verify the timestamp is in UTC by checking round-trip.
	if !e.OccurredAt.Equal(e.OccurredAt.UTC()) {
		return fmt.Errorf("%w: occurred_at is not in UTC", ErrInvalidInput)
	}
	if e.PID != 0 && e.PID < 1 {
		return fmt.Errorf("%w: pid must be positive when present, got %d", ErrInvalidInput, e.PID)
	}
	if !isValidSource(e.Source) {
		return fmt.Errorf("%w: unknown source %q", ErrInvalidInput, e.Source)
	}
	if !isValidCapability(e.Capability) {
		return fmt.Errorf("%w: unknown capability %q", ErrInvalidInput, e.Capability)
	}
	if !isValidSeverity(e.Severity) {
		return fmt.Errorf("%w: unknown severity %q", ErrInvalidInput, e.Severity)
	}
	if utf8.RuneCountInString(e.Project) > maxProjectLen {
		return fmt.Errorf("%w: project exceeds max length of %d runes", ErrInvalidInput, maxProjectLen)
	}
	if utf8.RuneCountInString(e.CWD) > maxCwdLen {
		return fmt.Errorf("%w: cwd exceeds max length of %d runes", ErrInvalidInput, maxCwdLen)
	}
	if utf8.RuneCountInString(e.Title) > maxTitleLen {
		return fmt.Errorf("%w: title exceeds max length of %d runes", ErrInvalidInput, maxTitleLen)
	}
	if utf8.RuneCountInString(e.Message) > maxMessageLen {
		return fmt.Errorf("%w: message exceeds max length of %d runes", ErrInvalidInput, maxMessageLen)
	}
	if e.Metadata != nil && len(e.Metadata) > maxMetadataCount {
		return fmt.Errorf("%w: metadata has %d entries, max is %d", ErrInvalidInput, len(e.Metadata), maxMetadataCount)
	}
	for k, v := range e.Metadata {
		if utf8.RuneCountInString(k) > maxEventIDLen {
			return fmt.Errorf("%w: metadata key %q exceeds max length", ErrInvalidInput, k)
		}
		if utf8.RuneCountInString(v) > maxMetadataValue {
			return fmt.Errorf("%w: metadata value for key %q exceeds max length", ErrInvalidInput, k)
		}
	}
	return nil
}

func isValidEventKind(k EventKind) bool {
	switch k {
	case EventSessionStarted, EventWorkStarted, EventProgress,
		EventInputRequired, EventTurnCompleted, EventFailed,
		EventSessionEnded, EventCleared:
		return true
	}
	return false
}

func isValidSource(s Source) bool {
	switch s {
	case SourceHook, SourceNotify, SourceManual, SourceLog:
		return true
	}
	return false
}

func isValidCapability(c Capability) bool {
	switch c {
	case CapabilityFull, CapabilityCompletionOnly, CapabilityManual:
		return true
	}
	return false
}

func isValidSeverity(s string) bool {
	switch s {
	case "":
		return true
	case "info", "warning", "error":
		return true
	}
	return false
}

// SanitizeMessage removes control characters and trims the message to max length.
func SanitizeMessage(msg string) string {
	var b strings.Builder
	b.Grow(len(msg))
	for _, r := range msg {
		if r >= 32 && r != 127 {
			b.WriteRune(r)
		}
	}
	s := b.String()
	runes := []rune(s)
	if len(runes) > maxMessageLen {
		s = string(runes[:maxMessageLen])
	}
	return s
}
