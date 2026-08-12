package reducer_test

import (
	"testing"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
	"github.com/taskmaster-dev/taskmaster/internal/reducer"
)

var fixedTime = time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)

func statusPtr(s domain.Status) *domain.Status {
	return &s
}

func snapshot(status domain.Status, t time.Time) *domain.SessionSnapshot {
	ttl, _ := domain.DefaultTTL(status)
	return &domain.SessionSnapshot{
		SchemaVersion: 1,
		Revision:      1,
		Agent:         "claude",
		SessionIDHash: domain.SessionHash("sess-123"),
		SessionID:     "sess-123",
		Status:        status,
		StartedAt:     t,
		UpdatedAt:     t,
		ExpiresAt:     t.Add(ttl),
		LastEventID:   "last-event-1",
		Source:        domain.SourceHook,
		Capability:    domain.CapabilityFull,
	}
}

func validEvent(kind domain.EventKind, t time.Time) domain.Event {
	return domain.Event{
		SchemaVersion: 1,
		EventID:       "test-event-1",
		Agent:         "claude",
		SessionID:     "sess-123",
		Kind:          kind,
		OccurredAt:    t,
		Source:        domain.SourceHook,
		Capability:    domain.CapabilityFull,
	}
}

func TestReduceMapsLifecycleEvents(t *testing.T) {
	now := time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		old        *domain.SessionSnapshot
		kind       domain.EventKind
		wantStatus *domain.Status
		wantDelete bool
	}{
		{"start creates working", nil, domain.EventSessionStarted, statusPtr(domain.StatusWorking), false},
		{"permission waits", snapshot(domain.StatusWorking, now), domain.EventInputRequired, statusPtr(domain.StatusWaitingInput), false},
		{"progress resumes waiting", snapshot(domain.StatusWaitingInput, now), domain.EventProgress, statusPtr(domain.StatusWorking), false},
		{"stop completes", snapshot(domain.StatusWorking, now), domain.EventTurnCompleted, statusPtr(domain.StatusCompleted), false},
		{"failure errors", snapshot(domain.StatusWorking, now), domain.EventFailed, statusPtr(domain.StatusError), false},
		{"session end deletes", snapshot(domain.StatusWorking, now), domain.EventSessionEnded, nil, true},
		{"clear missing is idempotent", nil, domain.EventCleared, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := validEvent(tt.kind, now)
			got, err := reducer.Reduce(tt.old, event)
			if err != nil {
				t.Fatalf("Reduce() error = %v", err)
			}
			if tt.wantDelete {
				if got.Next != nil {
					t.Fatalf("Reduce() Next = %v, want nil (delete)", got.Next)
				}
				if !got.Transition.ShouldDelete {
					t.Fatal("Reduce() Transition.ShouldDelete = false, want true")
				}
				return
			}
			if got.Next == nil {
				t.Fatalf("Reduce() Next = nil, want non-nil")
			}
			if got.Next.Status != *tt.wantStatus {
				t.Errorf("Reduce() Next.Status = %q, want %q", got.Next.Status, *tt.wantStatus)
			}
			if !got.Transition.StateChanged {
				t.Error("Reduce() Transition.StateChanged = false, want true")
			}
			// Revision starts at 1, increments by one
			if tt.old == nil {
				if got.Next.Revision != 1 {
					t.Errorf("Reduce() Next.Revision = %d, want 1 for new session", got.Next.Revision)
				}
			} else {
				if got.Next.Revision != tt.old.Revision+1 {
					t.Errorf("Reduce() Next.Revision = %d, want %d", got.Next.Revision, tt.old.Revision+1)
				}
			}
			// StartedAt preserved
			if tt.old != nil {
				if !got.Next.StartedAt.Equal(tt.old.StartedAt) {
					t.Errorf("Reduce() Next.StartedAt = %v, want %v (preserved)", got.Next.StartedAt, tt.old.StartedAt)
				}
			}
			// UpdatedAt equals event time
			if !got.Next.UpdatedAt.Equal(event.OccurredAt) {
				t.Errorf("Reduce() Next.UpdatedAt = %v, want %v", got.Next.UpdatedAt, event.OccurredAt)
			}
			// ExpiresAt equals event time plus target TTL
			wantTTL, _ := domain.DefaultTTL(*tt.wantStatus)
			wantExpires := event.OccurredAt.Add(wantTTL)
			if !got.Next.ExpiresAt.Equal(wantExpires) {
				t.Errorf("Reduce() Next.ExpiresAt = %v, want %v", got.Next.ExpiresAt, wantExpires)
			}
		})
	}
}
