package reducer

import (
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

// Reduce applies a single event to an old snapshot, producing a new snapshot or deletion.
// It is a pure function: it receives all time through event.OccurredAt, never calls time.Now,
// does not mutate old, and performs no I/O.
func Reduce(old *domain.SessionSnapshot, event domain.Event) (domain.ReduceResult, error) {
	if err := event.Validate(); err != nil {
		return domain.ReduceResult{}, err
	}

	// Idempotency: same event ID as last, return old without incrementing revision.
	if old != nil && old.LastEventID == event.EventID {
		return domain.ReduceResult{
			Next:       domain.CopySessionSnapshot(old),
			Transition: domain.Transition{IgnoredAsDuplicate: true},
		}, nil
	}

	// Stale event: occurred more than 2s before old updated_at.
	if old != nil && event.OccurredAt.Before(old.UpdatedAt.Add(-2*time.Second)) {
		return domain.ReduceResult{}, domain.ErrStaleEvent
	}

	// Delete events: session_ended and cleared.
	if event.Kind == domain.EventSessionEnded || event.Kind == domain.EventCleared {
		t := domain.Transition{ShouldDelete: true}
		if old != nil {
			t.From = &old.Status
			t.StateChanged = true
		}
		return domain.ReduceResult{Next: nil, Transition: t}, nil
	}

	// Map event kind to new status.
	var newStatus domain.Status
	switch event.Kind {
	case domain.EventSessionStarted, domain.EventWorkStarted, domain.EventProgress:
		newStatus = domain.StatusWorking
	case domain.EventInputRequired:
		newStatus = domain.StatusWaitingInput
	case domain.EventTurnCompleted:
		newStatus = domain.StatusCompleted
	case domain.EventFailed:
		newStatus = domain.StatusError
	default:
		return domain.ReduceResult{}, domain.ErrUnsupportedEvent
	}

	// Compute TTL for the new status.
	ttl, err := domain.DefaultTTL(newStatus)
	if err != nil {
		return domain.ReduceResult{}, err
	}

	// Build the next snapshot.
	next := &domain.SessionSnapshot{
		SchemaVersion: domain.SchemaVersion,
		Agent:         event.Agent,
		SessionIDHash: domain.SessionHash(event.SessionID),
		SessionID:     event.SessionID,
		Status:        newStatus,
		Project:       event.Project,
		CWD:           event.CWD,
		Title:         event.Title,
		Message:       domain.SanitizeMessage(event.Message),
		PID:           event.PID,
		Source:        event.Source,
		Capability:    event.Capability,
		UpdatedAt:     event.OccurredAt,
		ExpiresAt:     event.OccurredAt.Add(ttl),
		LastEventID:   event.EventID,
	}
	if old != nil {
		next.Revision = old.Revision + 1
		next.StartedAt = old.StartedAt
	} else {
		next.Revision = 1
		next.StartedAt = event.OccurredAt
	}

	// Error fingerprint: only on failed events.
	if event.Kind == domain.EventFailed {
		fingerprint := domain.ErrorFingerprintFromMessage(event.Message)
		next.LastErrorFingerprint = string(fingerprint)
	}

	// Clear error fingerprint when transitioning away from error.
	if old != nil && old.Status == domain.StatusError && newStatus != domain.StatusError {
		next.LastErrorFingerprint = ""
	}

	// Build transition.
	t := domain.Transition{
		StateChanged: old == nil || old.Status != newStatus,
		ShouldDelete: false,
	}
	if old != nil {
		t.From = &old.Status
		t.ErrorChanged = old.LastErrorFingerprint != next.LastErrorFingerprint
	}
	if newStatus == domain.StatusError {
		t.To = &newStatus
	} else {
		to := newStatus
		t.To = &to
	}
	return domain.ReduceResult{Next: next, Transition: t}, nil
}
