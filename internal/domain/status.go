package domain

import (
	"fmt"
	"time"
)

const SchemaVersion = 1

// Status represents the persistent state of a session snapshot.
type Status string

const (
	StatusWorking      Status = "working"
	StatusWaitingInput Status = "waiting_input"
	StatusCompleted    Status = "completed"
	StatusError        Status = "error"
)

// DefaultTTL returns the default time-to-live for the given status.
// It returns ErrInvalidInput for unknown statuses.
func DefaultTTL(s Status) (time.Duration, error) {
	switch s {
	case StatusWorking:
		return 15 * time.Minute, nil
	case StatusWaitingInput:
		return 8 * time.Hour, nil
	case StatusCompleted:
		return 2 * time.Minute, nil
	case StatusError:
		return 8 * time.Hour, nil
	default:
		return 0, fmt.Errorf("%w: unknown status %q", ErrInvalidInput, s)
	}
}

// Valid reports whether s is one of the defined status constants.
func (s Status) Valid() bool {
	switch s {
	case StatusWorking, StatusWaitingInput, StatusCompleted, StatusError:
		return true
	}
	return false
}
