package schemas_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
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
	props, ok := schema["properties"].(map[string]interface{})
	if !ok {
		t.Fatalf("schema has no properties")
	}
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
	_ = props // unused but ensures properties exists
}

func TestSchemasAreValidJSONAndMatchDomainEnums(t *testing.T) {
	event := loadSchema(t, "event.schema.json")
	snapshot := loadSchema(t, "session-snapshot.schema.json")

	assertEnum(t, event, "kind", []string{
		"session_started", "work_started", "progress", "input_required",
		"turn_completed", "failed", "session_ended", "cleared",
	})
	assertEnum(t, event, "capability", []string{"full", "completion_only", "manual"})
	assertEnum(t, snapshot, "status", []string{"working", "waiting_input", "completed", "error"})

	// capability must be present in each schema's required list
	assertRequiredContains(t, event, "capability")
	assertRequiredContains(t, snapshot, "capability")
}
