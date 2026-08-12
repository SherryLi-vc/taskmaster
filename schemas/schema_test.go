package schemas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/taskmaster-dev/taskmaster/internal/domain"
)

func loadSchema(t *testing.T, name string) map[string]interface{} {
	t.Helper()
	_, b, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(b), name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", name, err)
	}
	var schema map[string]interface{}
	if err := json.Unmarshal(data, &schema); err != nil {
		t.Fatalf("Unmarshal(%q) error = %v", name, err)
	}
	return schema
}

func assertEnum(t *testing.T, schema map[string]interface{}, key string, want []string) {
	t.Helper()
	props, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema has no properties")
	}
	prop, ok := props[key].(map[string]interface{})
	if !ok {
		t.Fatalf("schema properties missing key %q", key)
	}
	enumVal, ok := prop["enum"].([]interface{})
	if !ok {
		t.Fatalf("schema properties[%q] has no enum", key)
	}
	got := make([]string, len(enumVal))
	for i, v := range enumVal {
		s, ok := v.(string)
		if !ok {
			t.Fatalf("schema properties[%q] enum[%d] is not a string", key, i)
		}
		got[i] = s
	}
	if len(got) != len(want) {
		t.Fatalf("schema properties[%q] enum = %v, want %v", key, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("schema properties[%q] enum[%d] = %q, want %q", key, i, got[i], want[i])
		}
	}
}

func assertRequiredContains(t *testing.T, schema map[string]interface{}, key string) {
	t.Helper()
	required, ok := schema["required"].([]interface{})
	if !ok {
		t.Fatalf("schema has no required")
	}
	found := false
	for _, r := range required {
		if r == key {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("schema required does not contain %q", key)
	}
}

// enumValues returns the string values of domain.EventKind constants in order.
func eventKinds() []string {
	return []string{
		string(domain.EventSessionStarted),
		string(domain.EventWorkStarted),
		string(domain.EventProgress),
		string(domain.EventInputRequired),
		string(domain.EventTurnCompleted),
		string(domain.EventFailed),
		string(domain.EventSessionEnded),
		string(domain.EventCleared),
	}
}

// enumValues returns the string values of domain.Source constants in order.
func sources() []string {
	return []string{
		string(domain.SourceHook),
		string(domain.SourceNotify),
		string(domain.SourceManual),
		string(domain.SourceLog),
	}
}

// enumValues returns the string values of domain.Capability constants in order.
func capabilities() []string {
	return []string{
		string(domain.CapabilityFull),
		string(domain.CapabilityCompletionOnly),
		string(domain.CapabilityManual),
	}
}

// enumValues returns the string values of domain.Status constants in order.
func statuses() []string {
	return []string{
		string(domain.StatusWorking),
		string(domain.StatusWaitingInput),
		string(domain.StatusCompleted),
		string(domain.StatusError),
	}
}

func TestSchemasAreValidJSONAndMatchDomainEnums(t *testing.T) {
	event := loadSchema(t, "event.schema.json")
	snapshot := loadSchema(t, "session-snapshot.schema.json")

	// Event enums must match domain constants exactly.
	assertEnum(t, event, "kind", eventKinds())
	assertEnum(t, event, "source", sources())
	assertEnum(t, event, "capability", capabilities())

	// Snapshot enums must match domain constants exactly.
	assertEnum(t, snapshot, "status", statuses())
	assertEnum(t, snapshot, "source", sources())
	assertEnum(t, snapshot, "capability", capabilities())

	// Severity enum in event schema.
	assertEnum(t, event, "severity", []string{"info", "warning", "error"})
}

func TestSchemasRequiredFields(t *testing.T) {
	event := loadSchema(t, "event.schema.json")
	snapshot := loadSchema(t, "session-snapshot.schema.json")

	// Event required fields.
	for _, field := range []string{
		"schema_version", "event_id", "agent", "session_id",
		"kind", "occurred_at", "source", "capability",
	} {
		assertRequiredContains(t, event, field)
	}

	// Snapshot required fields (including session_id per Finding #4).
	for _, field := range []string{
		"schema_version", "revision", "agent", "session_id_hash",
		"session_id", "status", "started_at", "updated_at", "expires_at",
		"last_event_id", "source", "capability",
	} {
		assertRequiredContains(t, snapshot, field)
	}
}

func TestSchemasSchemaVersionConst(t *testing.T) {
	event := loadSchema(t, "event.schema.json")
	snapshot := loadSchema(t, "session-snapshot.schema.json")

	// Both schemas must have schema_version const equal to 1.
	for name, schema := range map[string]map[string]interface{}{
		"event":            event,
		"session-snapshot": snapshot,
	} {
		props, ok := schema["properties"].(map[string]interface{})
		if !ok {
			t.Fatalf("%s schema has no properties", name)
		}
		version, ok := props["schema_version"].(map[string]interface{})
		if !ok {
			t.Fatalf("%s schema missing schema_version property", name)
		}
		constVal, ok := version["const"]
		if !ok {
			t.Fatalf("%s schema.schema_version missing const", name)
		}
		if constVal.(float64) != float64(domain.SchemaVersion) {
			t.Errorf("%s schema_version.const = %v, want %d", name, constVal, domain.SchemaVersion)
		}
	}
}

func TestEventSchemaMetadataPropertyNamesMaxLength(t *testing.T) {
	event := loadSchema(t, "event.schema.json")
	props, ok := event["properties"].(map[string]interface{})
	if !ok {
		t.Fatal("event schema has no properties")
	}
	meta, ok := props["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("event schema missing metadata property")
	}
	pn, ok := meta["propertyNames"].(map[string]interface{})
	if !ok {
		t.Fatal("event.schema.metadata missing propertyNames")
	}
	got, ok := pn["maxLength"].(float64)
	if !ok {
		t.Fatal("event.schema.metadata.propertyNames missing maxLength")
	}
	if got != 128 {
		t.Errorf("metadata.propertyNames.maxLength = %v, want 128", got)
	}
}
