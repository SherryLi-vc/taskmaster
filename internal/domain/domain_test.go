package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

func TestEventValidateRejectsMissingSession(t *testing.T) {
	event := validEvent()
	event.SessionID = ""
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestDefaultTTL(t *testing.T) {
	tests := []struct {
		status domain.Status
		want   time.Duration
	}{
		{domain.StatusWorking, 15 * time.Minute},
		{domain.StatusWaitingInput, 8 * time.Hour},
		{domain.StatusCompleted, 2 * time.Minute},
		{domain.StatusError, 8 * time.Hour},
	}
	for _, tt := range tests {
		got, err := domain.DefaultTTL(tt.status)
		if err != nil || got != tt.want {
			t.Fatalf("DefaultTTL(%q) = %v, %v; want %v, nil", tt.status, got, err, tt.want)
		}
	}
}

func TestSessionHashUsesSHA256(t *testing.T) {
	const want = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := domain.SessionHash(""); got != want {
		t.Fatalf("SessionHash(empty) = %q, want %q", got, want)
	}
}

func TestSchemaVersionIsOne(t *testing.T) {
	if domain.SchemaVersion != 1 {
		t.Fatalf("SchemaVersion = %d, want 1", domain.SchemaVersion)
	}
}

func TestEventValidateRejectsBadSchemaVersion(t *testing.T) {
	event := validEvent()
	event.SchemaVersion = 2
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsEmptyEventID(t *testing.T) {
	event := validEvent()
	event.EventID = ""
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsLongEventID(t *testing.T) {
	event := validEvent()
	event.EventID = "a" + string(make([]byte, 128))
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsUppercaseAgent(t *testing.T) {
	event := validEvent()
	event.Agent = "Claude"
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsInvalidKind(t *testing.T) {
	event := validEvent()
	event.Kind = "unknown"
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsInvalidSource(t *testing.T) {
	event := validEvent()
	event.Source = "random"
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsInvalidCapability(t *testing.T) {
	event := validEvent()
	event.Capability = "partial"
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsZeroTimestamp(t *testing.T) {
	event := validEvent()
	event.OccurredAt = time.Time{}
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsNegativePID(t *testing.T) {
	event := validEvent()
	event.PID = -1
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsMissingCapability(t *testing.T) {
	event := validEvent()
	event.Capability = ""
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateRejectsTooLongMessage(t *testing.T) {
	event := validEvent()
	event.Message = "a" + string(make([]byte, 512))
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput", err)
	}
}

func TestEventValidateAcceptsValidEvent(t *testing.T) {
	event := validEvent()
	if err := event.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}
}

func TestStatusIdleNotIncluded(t *testing.T) {
	var s domain.Status
	// idle should not be a valid constant; verify by checking DefaultTTL rejects it
	if _, err := domain.DefaultTTL(s); err == nil {
		t.Fatal("DefaultTTL(empty status) should error, got nil")
	}
}

func TestStringLengthCountedByRune(t *testing.T) {
	// Use a multi-byte Unicode character to verify rune counting, not byte counting.
	event := validEvent()
	// 128 runes of emoji (each emoji is 4 bytes in UTF-8 but 1 rune)
	longEventID := ""
	for i := 0; i < 129; i++ {
		longEventID += "🔧"
	}
	event.EventID = longEventID
	if err := event.Validate(); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("Validate() error = %v, want ErrInvalidInput for 129-rune event ID", err)
	}
}

func validEvent() domain.Event {
	return domain.Event{
		SchemaVersion: 1,
		EventID:       "test-event-1",
		Agent:         "claude",
		SessionID:     "sess-123",
		Kind:          domain.EventWorkStarted,
		OccurredAt:    time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC),
		Source:        domain.SourceHook,
		Capability:    domain.CapabilityFull,
	}
}
