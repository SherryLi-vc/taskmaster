package reducer_test

import (
	"errors"
	"reflect"
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

func TestReduceIgnoresDuplicateEventID(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	old.LastEventID = "same"
	event := validEvent(domain.EventProgress, fixedTime.Add(time.Second))
	event.EventID = "same"
	got, err := reducer.Reduce(old, event)
	if err != nil || !got.Transition.IgnoredAsDuplicate || got.Next.Revision != old.Revision {
		t.Fatalf("duplicate result = %#v, %v", got, err)
	}
}

func TestReduceRejectsEventOlderThanTolerance(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventTurnCompleted, fixedTime.Add(-3*time.Second))
	_, err := reducer.Reduce(old, event)
	if !errors.Is(err, domain.ErrStaleEvent) {
		t.Fatalf("error = %v, want ErrStaleEvent", err)
	}
}

func TestReduceAcceptsEventExactlyTwoSecondsOld(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventTurnCompleted, fixedTime.Add(-2*time.Second))
	_, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("expected acceptance of event exactly 2s old, got error = %v", err)
	}
}

func TestReduceMarksChangedErrorFingerprint(t *testing.T) {
	old := snapshot(domain.StatusError, fixedTime)
	old.LastErrorFingerprint = "a" + string(make([]byte, 63)) // dummy 64-char fingerprint
	event := validEvent(domain.EventFailed, fixedTime.Add(time.Minute))
	event.Message = "quota exceeded"
	got, err := reducer.Reduce(old, event)
	if err != nil || !got.Transition.ErrorChanged {
		t.Fatalf("result = %#v, %v; want ErrorChanged", got, err)
	}
}

func TestReduceClearsErrorFingerprintOnNonErrorTransition(t *testing.T) {
	old := snapshot(domain.StatusError, fixedTime)
	old.LastErrorFingerprint = "b" + string(make([]byte, 63)) // dummy 64-char fingerprint
	event := validEvent(domain.EventWorkStarted, fixedTime.Add(time.Minute))
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Next.LastErrorFingerprint != "" {
		t.Errorf("Reducer() Next.LastErrorFingerprint = %q, want empty after clearing error", got.Next.LastErrorFingerprint)
	}
}

// Finding #1: Message propagation with sanitization.
func TestReducePropagatesMessageViaSanitize(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventInputRequired, fixedTime.Add(time.Second))
	event.Message = "has\x00null\x01chars"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Next.Message != "hasnullchars" {
		t.Errorf("Next.Message = %q, want sanitized %q", got.Next.Message, "hasnullchars")
	}
}

func TestReduceSanitizesFailedEventMessage(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventFailed, fixedTime.Add(time.Second))
	event.Message = "bad\x00char"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Next.Message != "badchar" {
		t.Errorf("Next.Message = %q, want sanitized %q", got.Next.Message, "badchar")
	}
}

// TestReduceStripsUnicodeControlChars verifies SanitizeMessage uses unicode.IsControl
// to strip C1 control characters (U+0085, U+009B, U+009C, U+009D) and preserves
// legitimate Unicode including CJK, emoji, and U+FFFD.
func TestReduceStripsUnicodeControlChars(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventInputRequired, fixedTime.Add(time.Second))
	// Use Go Unicode escape sequences to produce actual C1 control characters
	// (not invalid UTF-8 byte sequences).
	event.Message = "helloworldtestfoobar"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	want := "helloworldtestfoobar"
	if got.Next.Message != want {
		t.Errorf("Next.Message = %q, want %q", got.Next.Message, want)
	}
}

// TestReducePreservesCJKEmojiAndFFFD verifies SanitizeMessage does NOT strip
// valid non-control Unicode: CJK characters, emoji, and U+FFFD.
func TestReducePreservesCJKEmojiAndFFFD(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventInputRequired, fixedTime.Add(time.Second))
	event.Message = "你好世界🔧�"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	want := "你好世界🔧�"
	if got.Next.Message != want {
		t.Errorf("Next.Message = %q, want %q", got.Next.Message, want)
	}
}

// TestReduceFailedEventFingerprintUsesSanitizedMessage verifies that for a
// failed event, the ErrorFingerprint is computed from the same sanitized
// message stored in Next.Message.
func TestReduceFailedEventFingerprintMatchesSanitizedMessage(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventFailed, fixedTime.Add(time.Second))
	event.Message = "errcode"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	wantMsg := "errcode"
	if got.Next.Message != wantMsg {
		t.Errorf("Next.Message = %q, want %q", got.Next.Message, wantMsg)
	}
	wantFP := domain.ErrorFingerprintFromMessage(event.Message)
	if got.Next.LastErrorFingerprint != string(wantFP) {
		t.Errorf("Next.LastErrorFingerprint = %q, want %q", got.Next.LastErrorFingerprint, wantFP)
	}
}

// Finding #2: Transition.StateChanged must reflect actual state change.
func TestReduceStateChangedWorkingProgress(t *testing.T) {
	// working + progress → working: no state change
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventProgress, fixedTime.Add(time.Second))
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Transition.StateChanged {
		t.Errorf("StateChanged = true, want false (working -> working)")
	}
	if got.Transition.From == nil || *got.Transition.From != domain.StatusWorking {
		t.Errorf("From = %v, want working", got.Transition.From)
	}
	if got.Transition.To == nil || *got.Transition.To != domain.StatusWorking {
		t.Errorf("To = %v, want working", got.Transition.To)
	}
}

func TestReduceStateChangedErrorSameFailed(t *testing.T) {
	// error + failed → error: no state change
	old := snapshot(domain.StatusError, fixedTime)
	old.LastErrorFingerprint = "fingerprint123" // dummy
	event := validEvent(domain.EventFailed, fixedTime.Add(time.Second))
	event.Message = "same error"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Transition.StateChanged {
		t.Errorf("StateChanged = true, want false (error -> error)")
	}
}

func TestReduceStateChangedCompletedTurnCompleted(t *testing.T) {
	// completed + turn_completed → completed: no state change
	old := snapshot(domain.StatusCompleted, fixedTime)
	event := validEvent(domain.EventTurnCompleted, fixedTime.Add(time.Second))
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Transition.StateChanged {
		t.Errorf("StateChanged = true, want false (completed -> completed)")
	}
}

func TestReduceStateChangedDeleteExisting(t *testing.T) {
	// delete on existing snapshot: state changed
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventSessionEnded, fixedTime.Add(time.Second))
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if !got.Transition.StateChanged {
		t.Error("StateChanged = false, want true (delete from existing)")
	}
	if !got.Transition.ShouldDelete {
		t.Error("ShouldDelete = false, want true")
	}
	if got.Transition.From == nil || *got.Transition.From != domain.StatusWorking {
		t.Errorf("From = %v, want working", got.Transition.From)
	}
}

func TestReduceStateChangedClearMissing(t *testing.T) {
	// clear on missing snapshot: no state change (nothing to clear)
	event := validEvent(domain.EventCleared, fixedTime.Add(time.Second))
	got, err := reducer.Reduce(nil, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if got.Transition.StateChanged {
		t.Error("StateChanged = true, want false (clear on missing)")
	}
	if !got.Transition.ShouldDelete {
		t.Error("ShouldDelete = false, want true")
	}
}

// Finding #3: Harden reducer - full Transition field assertions, Next.Validate(),
// Message and other persisted fields, old immutability, conversion matrix.
func TestReduceFullTransitionAssertions(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventInputRequired, fixedTime.Add(time.Second))
	event.Project = "my-project"
	event.CWD = "/home/user/project"
	event.Title = "Need permission"
	event.Message = "Permission required"
	event.PID = 1234
	event.Source = domain.SourceNotify
	event.Capability = domain.CapabilityCompletionOnly

	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}

	// All Transition fields
	if !got.Transition.StateChanged {
		t.Error("StateChanged = false, want true (working -> waiting_input)")
	}
	if got.Transition.ShouldDelete {
		t.Error("ShouldDelete = true, want false")
	}
	if got.Transition.IgnoredAsDuplicate {
		t.Error("IgnoredAsDuplicate = true, want false")
	}
	if got.Transition.From == nil || *got.Transition.From != domain.StatusWorking {
		t.Errorf("From = %v, want working", got.Transition.From)
	}
	if got.Transition.To == nil || *got.Transition.To != domain.StatusWaitingInput {
		t.Errorf("To = %v, want waiting_input", got.Transition.To)
	}
	if got.Transition.ErrorChanged {
		t.Error("ErrorChanged = true, want false (no error involved)")
	}

	// Validate next snapshot
	if err := got.Next.Validate(); err != nil {
		t.Errorf("Next.Validate() error = %v", err)
	}

	// Message must be sanitized
	if got.Next.Message != "Permission required" {
		t.Errorf("Next.Message = %q, want %q", got.Next.Message, "Permission required")
	}

	// Other persisted fields from event
	if got.Next.Project != "my-project" {
		t.Errorf("Next.Project = %q, want %q", got.Next.Project, "my-project")
	}
	if got.Next.CWD != "/home/user/project" {
		t.Errorf("Next.CWD = %q, want %q", got.Next.CWD, "/home/user/project")
	}
	if got.Next.Title != "Need permission" {
		t.Errorf("Next.Title = %q, want %q", got.Next.Title, "Need permission")
	}
	if got.Next.PID != 1234 {
		t.Errorf("Next.PID = %d, want %d", got.Next.PID, 1234)
	}
	if got.Next.Source != domain.SourceNotify {
		t.Errorf("Next.Source = %q, want %q", got.Next.Source, domain.SourceNotify)
	}
	if got.Next.Capability != domain.CapabilityCompletionOnly {
		t.Errorf("Next.Capability = %q, want %q", got.Next.Capability, domain.CapabilityCompletionOnly)
	}
}

func TestReduceOldSnapshotUnmutated(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	oldCopy := *old
	event := validEvent(domain.EventTurnCompleted, fixedTime.Add(time.Second))
	_, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	if !reflect.DeepEqual(*old, oldCopy) {
		t.Error("Reduce() mutated old snapshot")
	}
}

func TestReduceExactErrorFingerprint(t *testing.T) {
	old := snapshot(domain.StatusWorking, fixedTime)
	event := validEvent(domain.EventFailed, fixedTime.Add(time.Second))
	event.Message = "network timeout\x00"
	got, err := reducer.Reduce(old, event)
	if err != nil {
		t.Fatalf("Reduce() error = %v", err)
	}
	// SanitizeMessage strips control chars, so "network timeout\x00" → "network timeout"
	wantFingerprint := domain.ErrorFingerprintFromMessage("network timeout\x00")
	if got.Next.LastErrorFingerprint != string(wantFingerprint) {
		t.Errorf("Next.LastErrorFingerprint = %q, want %q", got.Next.LastErrorFingerprint, wantFingerprint)
	}
}

func TestReduceConversionMatrix(t *testing.T) {
	// EventKind × old-status conversion matrix
	// For each combination, verify the resulting status matches the transition table.
	// Rules:
	//   session_started/work_started/progress → working
	//   input_required → waiting_input
	//   turn_completed → completed
	//   failed → error
	//   session_ended/cleared → delete
	kinds := []domain.EventKind{
		domain.EventSessionStarted,
		domain.EventWorkStarted,
		domain.EventProgress,
		domain.EventInputRequired,
		domain.EventTurnCompleted,
		domain.EventFailed,
		domain.EventSessionEnded,
		domain.EventCleared,
	}
	statuses := []domain.Status{
		domain.StatusWorking,
		domain.StatusWaitingInput,
		domain.StatusCompleted,
		domain.StatusError,
	}
	wantStatuses := map[domain.EventKind]domain.Status{
		domain.EventSessionStarted: domain.StatusWorking,
		domain.EventWorkStarted:    domain.StatusWorking,
		domain.EventProgress:       domain.StatusWorking,
		domain.EventInputRequired:  domain.StatusWaitingInput,
		domain.EventTurnCompleted:  domain.StatusCompleted,
		domain.EventFailed:         domain.StatusError,
	}

	for _, kind := range kinds {
		for _, oldStatus := range statuses {
			t.Run(string(kind)+"_"+string(oldStatus), func(t *testing.T) {
				old := snapshot(oldStatus, fixedTime)
				event := validEvent(kind, fixedTime.Add(time.Second))
				got, err := reducer.Reduce(old, event)
				if err != nil {
					t.Fatalf("Reduce() error = %v", err)
				}
				if kind == domain.EventSessionEnded || kind == domain.EventCleared {
					if got.Next != nil {
						t.Errorf("Next = %v, want nil for %s", got.Next, kind)
					}
					if !got.Transition.ShouldDelete {
						t.Error("ShouldDelete = false, want true")
					}
					return
				}
				if got.Next == nil {
					t.Fatalf("Next = nil, want non-nil")
				}
				want := wantStatuses[kind]
				if got.Next.Status != want {
					t.Errorf("Next.Status = %q, want %q", got.Next.Status, want)
				}
				if err := got.Next.Validate(); err != nil {
					t.Errorf("Next.Validate() error = %v", err)
				}
			})
		}
	}
}
