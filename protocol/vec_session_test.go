package protocol

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
)

func TestExtractVecSessionVarUpdates_Empty(t *testing.T) {
	// Non-SET statements return nil, no error.
	updates, err := ExtractVecSessionVarUpdates("SELECT 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 0 {
		t.Errorf("expected 0 updates, got %d", len(updates))
	}
}

func TestExtractVecSessionVarUpdates_NonVecSet(t *testing.T) {
	// SET @@autocommit is not a marmot_vec var — returns empty.
	updates, err := ExtractVecSessionVarUpdates("SET @@autocommit = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 0 {
		t.Errorf("expected 0 updates for non-vec SET, got %d", len(updates))
	}
}

func TestExtractVecSessionVarUpdates_SingleIntVar(t *testing.T) {
	updates, err := ExtractVecSessionVarUpdates("SET @@marmot_vec_nprobe = 64")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Name != "marmot_vec_nprobe" {
		t.Errorf("Name = %q, want %q", updates[0].Name, "marmot_vec_nprobe")
	}
	if updates[0].Value != "64" {
		t.Errorf("Value = %q, want %q", updates[0].Value, "64")
	}
}

func TestExtractVecSessionVarUpdates_StringVar(t *testing.T) {
	updates, err := ExtractVecSessionVarUpdates("SET @@marmot_vec_force_plan = 'pre'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Name != "marmot_vec_force_plan" {
		t.Errorf("Name = %q, want %q", updates[0].Name, "marmot_vec_force_plan")
	}
	if updates[0].Value != "pre" {
		t.Errorf("Value = %q, want %q", updates[0].Value, "pre")
	}
}

func TestExtractVecSessionVarUpdates_FloatVar(t *testing.T) {
	updates, err := ExtractVecSessionVarUpdates("SET @@marmot_vec_retrain_growth_ratio = 2.5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}
	if updates[0].Value != "2.5" {
		t.Errorf("Value = %q, want %q", updates[0].Value, "2.5")
	}
}

func TestExtractVecSessionVarUpdates_MultiVar(t *testing.T) {
	// SET with multiple vars: only marmot_vec_* should appear in updates.
	updates, err := ExtractVecSessionVarUpdates(
		"SET @@marmot_vec_nprobe = 32, @@marmot_vec_fallback = 'off'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 2 {
		t.Fatalf("expected 2 updates, got %d", len(updates))
	}
	names := map[string]string{}
	for _, u := range updates {
		names[u.Name] = u.Value
	}
	if names["marmot_vec_nprobe"] != "32" {
		t.Errorf("marmot_vec_nprobe = %q, want %q", names["marmot_vec_nprobe"], "32")
	}
	if names["marmot_vec_fallback"] != "off" {
		t.Errorf("marmot_vec_fallback = %q, want %q", names["marmot_vec_fallback"], "off")
	}
}

func TestExtractVecSessionVarUpdates_MixedVars(t *testing.T) {
	// Non-vec vars are silently ignored; only marmot_vec_* are returned.
	updates, err := ExtractVecSessionVarUpdates(
		"SET @@autocommit = 1, @@marmot_vec_prefilter_cap = 3000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update (only vec var), got %d", len(updates))
	}
	if updates[0].Name != "marmot_vec_prefilter_cap" || updates[0].Value != "3000" {
		t.Errorf("unexpected update: %+v", updates[0])
	}
}

// TestApplyVecSessionVarUpdates ensures the full pipeline from SQL → session works.
func TestApplyVecSessionVarUpdates(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars()

	updates, err := ExtractVecSessionVarUpdates("SET @@marmot_vec_nprobe = 128")
	if err != nil {
		t.Fatalf("extract error: %v", err)
	}

	for _, u := range updates {
		if err := vars.Apply(u.Name, u.Value); err != nil {
			t.Fatalf("apply error: %v", err)
		}
	}

	if vars.Nprobe != 128 {
		t.Errorf("Nprobe = %d, want 128", vars.Nprobe)
	}
}

// TestApplyVecSessionVarUpdates_InvalidValue ensures Apply returns an error for bad values
// and the caller can surface MARMOT-VEC-012.
func TestApplyVecSessionVarUpdates_InvalidForcePlan(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars()

	updates, err := ExtractVecSessionVarUpdates("SET @@marmot_vec_force_plan = 'invalid'")
	if err != nil {
		t.Fatalf("extract error: %v", err)
	}
	if len(updates) != 1 {
		t.Fatalf("expected 1 update, got %d", len(updates))
	}

	applyErr := vars.Apply(updates[0].Name, updates[0].Value)
	if applyErr == nil {
		t.Error("expected MARMOT-VEC-012 error for invalid force_plan, got nil")
	}
}
