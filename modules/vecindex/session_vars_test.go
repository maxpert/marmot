package vecindex

import (
	"testing"
)

func TestDefaultVecSessionVars(t *testing.T) {
	v := DefaultVecSessionVars()

	if v.Nprobe != 0 {
		t.Errorf("Nprobe default = %d, want 0 (use index default)", v.Nprobe)
	}
	if v.ForcePlan != ForcePlanAuto {
		t.Errorf("ForcePlan default = %v, want ForcePlanAuto", v.ForcePlan)
	}
	if v.PrefilterCap != 5000 {
		t.Errorf("PrefilterCap default = %d, want 5000", v.PrefilterCap)
	}
	if !v.Fallback {
		t.Error("Fallback default = false, want true")
	}
	if v.DeltaFlushInterval != 10 {
		t.Errorf("DeltaFlushInterval default = %d, want 10", v.DeltaFlushInterval)
	}
	if v.DeltaMaxRows != 10000 {
		t.Errorf("DeltaMaxRows default = %d, want 10000", v.DeltaMaxRows)
	}
	if v.DeltaFlushBatch != 1000 {
		t.Errorf("DeltaFlushBatch default = %d, want 1000", v.DeltaFlushBatch)
	}
	if !v.RetrainEnabled {
		t.Error("RetrainEnabled default = false, want true")
	}
	if v.RetrainCheckInterval != 30 {
		t.Errorf("RetrainCheckInterval default = %d, want 30", v.RetrainCheckInterval)
	}
	if v.RetrainGrowthRatio != 1.5 {
		t.Errorf("RetrainGrowthRatio default = %f, want 1.5", v.RetrainGrowthRatio)
	}
	if v.RetrainDeltaRatio != 0.2 {
		t.Errorf("RetrainDeltaRatio default = %f, want 0.2", v.RetrainDeltaRatio)
	}
	if v.ReindexChunkRows != 10000 {
		t.Errorf("ReindexChunkRows default = %d, want 10000", v.ReindexChunkRows)
	}
}

func TestVecSessionVars_Apply_IntVars(t *testing.T) {
	tests := []struct {
		name    string
		varName string
		value   string
		check   func(v *VecSessionVars) bool
		wantErr bool
	}{
		{"nprobe", "marmot_vec_nprobe", "64", func(v *VecSessionVars) bool { return v.Nprobe == 64 }, false},
		{"prefilter_cap", "marmot_vec_prefilter_cap", "1000", func(v *VecSessionVars) bool { return v.PrefilterCap == 1000 }, false},
		{"delta_flush_interval", "marmot_vec_delta_flush_interval", "5", func(v *VecSessionVars) bool { return v.DeltaFlushInterval == 5 }, false},
		{"delta_max_rows", "marmot_vec_delta_max_rows", "500", func(v *VecSessionVars) bool { return v.DeltaMaxRows == 500 }, false},
		{"delta_flush_batch", "marmot_vec_delta_flush_batch", "200", func(v *VecSessionVars) bool { return v.DeltaFlushBatch == 200 }, false},
		{"retrain_check_interval", "marmot_vec_retrain_check_interval", "60", func(v *VecSessionVars) bool { return v.RetrainCheckInterval == 60 }, false},
		{"reindex_chunk_rows", "marmot_vec_reindex_chunk_rows", "5000", func(v *VecSessionVars) bool { return v.ReindexChunkRows == 5000 }, false},
		{"nprobe_negative", "marmot_vec_nprobe", "-1", nil, true},
		{"prefilter_cap_zero", "marmot_vec_prefilter_cap", "0", nil, true},
		{"nprobe_non_int", "marmot_vec_nprobe", "abc", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := DefaultVecSessionVars()
			err := v.Apply(tt.varName, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply(%q, %q) error = %v, wantErr %v", tt.varName, tt.value, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.check(&v) {
				t.Errorf("Apply(%q, %q): value not applied correctly", tt.varName, tt.value)
			}
		})
	}
}

func TestVecSessionVars_Apply_ForcePlan(t *testing.T) {
	tests := []struct {
		value   string
		want    ForcePlan
		wantErr bool
	}{
		{"auto", ForcePlanAuto, false},
		{"pre", ForcePlanPre, false},
		{"post", ForcePlanPost, false},
		{"AUTO", ForcePlanAuto, false},
		{"PRE", ForcePlanPre, false},
		{"POST", ForcePlanPost, false},
		{"invalid", 0, true},
		{"off", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		t.Run("force_plan="+tt.value, func(t *testing.T) {
			v := DefaultVecSessionVars()
			err := v.Apply("marmot_vec_force_plan", tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply force_plan=%q error = %v, wantErr %v", tt.value, err, tt.wantErr)
				return
			}
			if !tt.wantErr && v.ForcePlan != tt.want {
				t.Errorf("Apply force_plan=%q got %v, want %v", tt.value, v.ForcePlan, tt.want)
			}
		})
	}
}

func TestVecSessionVars_Apply_FallbackAndRetrain(t *testing.T) {
	tests := []struct {
		name    string
		varName string
		value   string
		check   func(v *VecSessionVars) bool
		wantErr bool
	}{
		{"fallback on", "marmot_vec_fallback", "on", func(v *VecSessionVars) bool { return v.Fallback }, false},
		{"fallback off", "marmot_vec_fallback", "off", func(v *VecSessionVars) bool { return !v.Fallback }, false},
		{"fallback ON uppercase", "marmot_vec_fallback", "ON", func(v *VecSessionVars) bool { return v.Fallback }, false},
		{"fallback invalid", "marmot_vec_fallback", "yes", nil, true},
		{"retrain on", "marmot_vec_retrain_enabled", "on", func(v *VecSessionVars) bool { return v.RetrainEnabled }, false},
		{"retrain off", "marmot_vec_retrain_enabled", "off", func(v *VecSessionVars) bool { return !v.RetrainEnabled }, false},
		{"retrain invalid", "marmot_vec_retrain_enabled", "1", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.varName+" "+tt.value, func(t *testing.T) {
			v := DefaultVecSessionVars()
			err := v.Apply(tt.varName, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply(%q, %q) error = %v, wantErr %v", tt.varName, tt.value, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.check(&v) {
				t.Errorf("Apply(%q, %q): value not applied correctly", tt.varName, tt.value)
			}
		})
	}
}

func TestVecSessionVars_Apply_FloatVars(t *testing.T) {
	tests := []struct {
		name    string
		varName string
		value   string
		check   func(v *VecSessionVars) bool
		wantErr bool
	}{
		{"growth_ratio", "marmot_vec_retrain_growth_ratio", "2.0", func(v *VecSessionVars) bool { return v.RetrainGrowthRatio == 2.0 }, false},
		{"delta_ratio", "marmot_vec_retrain_delta_ratio", "0.5", func(v *VecSessionVars) bool { return v.RetrainDeltaRatio == 0.5 }, false},
		{"growth_ratio_too_low", "marmot_vec_retrain_growth_ratio", "0.9", nil, true},
		{"delta_ratio_too_high", "marmot_vec_retrain_delta_ratio", "1.1", nil, true},
		{"growth_ratio_bad", "marmot_vec_retrain_growth_ratio", "abc", nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.varName+" "+tt.value, func(t *testing.T) {
			v := DefaultVecSessionVars()
			err := v.Apply(tt.varName, tt.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("Apply(%q, %q) error = %v, wantErr %v", tt.varName, tt.value, err, tt.wantErr)
				return
			}
			if !tt.wantErr && !tt.check(&v) {
				t.Errorf("Apply(%q, %q): value not applied correctly", tt.varName, tt.value)
			}
		})
	}
}

func TestVecSessionVars_Apply_UnknownVar(t *testing.T) {
	v := DefaultVecSessionVars()
	err := v.Apply("marmot_vec_unknown_setting", "42")
	if err == nil {
		t.Error("Apply(unknown var) expected error, got nil")
	}
}

func TestForcePlan_String(t *testing.T) {
	tests := []struct {
		plan ForcePlan
		want string
	}{
		{ForcePlanAuto, "auto"},
		{ForcePlanPre, "pre"},
		{ForcePlanPost, "post"},
	}
	for _, tt := range tests {
		if got := tt.plan.String(); got != tt.want {
			t.Errorf("ForcePlan(%d).String() = %q, want %q", tt.plan, got, tt.want)
		}
	}
}
