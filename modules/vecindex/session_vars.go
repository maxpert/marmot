package vecindex

import (
	"fmt"
	"strconv"
	"strings"
)

// ForcePlan controls the query plan selection strategy (§6.3).
type ForcePlan int

const (
	// ForcePlanAuto lets the cost-based planner decide.
	ForcePlanAuto ForcePlan = iota
	// ForcePlanPre forces pre-filter (exact brute-force over predicate results).
	ForcePlanPre
	// ForcePlanPost forces post-filter (IVF probe then predicate).
	ForcePlanPost
)

// String returns the SQL-level string for a ForcePlan.
func (p ForcePlan) String() string {
	switch p {
	case ForcePlanPre:
		return "pre"
	case ForcePlanPost:
		return "post"
	default:
		return "auto"
	}
}

// VecSessionVars holds per-connection @@marmot_vec_* session variables (§6.3, §6.4).
// Zero value is not valid; use DefaultVecSessionVars().
//
// All fields carry the current session value. Zero Nprobe means "use index default".
type VecSessionVars struct {
	// Nprobe is the number of IVF clusters probed per query (0 = index default).
	Nprobe int
	// ForcePlan overrides the cost-based plan selection.
	ForcePlan ForcePlan
	// PrefilterCap is the maximum pre-filter candidate set size.
	PrefilterCap int
	// Fallback enables short-result fallback (§7.5).
	Fallback bool
	// UseGoRank enables the Go-side ranking path that bypasses per-row UDF
	// dispatch on post-filter plans (§7.6).
	UseGoRank bool

	// Delta flush
	DeltaFlushInterval int // seconds between delta flush ticks
	DeltaMaxRows       int // maximum delta rows per flush cycle
	DeltaFlushBatch    int // rows per flush batch txn

	// Auto-retrain (§8.7)
	RetrainEnabled       bool
	RetrainCheckInterval int     // seconds between retrain checks
	RetrainGrowthRatio   float64 // per-cluster growth ratio threshold (>= 1.0)
	RetrainDeltaRatio    float64 // delta/total ratio threshold (0 < x <= 1.0)
	ReindexChunkRows     int     // rows per chunked-populate txn (§8.3)
}

// DefaultVecSessionVars returns session vars initialised to their documented defaults.
func DefaultVecSessionVars() VecSessionVars {
	return VecSessionVars{
		Nprobe:               0, // use index default (nprobe from __marmot_vector_indexes)
		ForcePlan:            ForcePlanAuto,
		PrefilterCap:         5000,
		Fallback:             true,
		UseGoRank:            true,
		DeltaFlushInterval:   10,
		DeltaMaxRows:         10000,
		DeltaFlushBatch:      1000,
		RetrainEnabled:       true,
		RetrainCheckInterval: 30,
		RetrainGrowthRatio:   1.5,
		RetrainDeltaRatio:    0.2,
		ReindexChunkRows:     10000,
	}
}

// Apply sets a single @@marmot_vec_* variable by its unprefixed name (e.g.,
// "marmot_vec_nprobe") and a string value extracted from the Vitess AST literal.
//
// Returns MARMOT-VEC-012 for an invalid enum value, a typed parse error for
// out-of-range numerics, and a generic error for unknown variable names.
func (v *VecSessionVars) Apply(name, value string) error {
	switch strings.ToLower(name) {
	case "marmot_vec_nprobe":
		n, err := parsePositiveInt(name, value, true) // 0 is OK (means "use default")
		if err != nil {
			return err
		}
		v.Nprobe = n

	case "marmot_vec_force_plan":
		fp, err := parseForcePlan(value)
		if err != nil {
			return err
		}
		v.ForcePlan = fp

	case "marmot_vec_prefilter_cap":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.PrefilterCap = n

	case "marmot_vec_fallback":
		b, err := parseOnOff(name, value)
		if err != nil {
			return err
		}
		v.Fallback = b

	case "marmot_vec_use_go_rank":
		b, err := parseOnOff(name, value)
		if err != nil {
			return err
		}
		v.UseGoRank = b

	case "marmot_vec_delta_flush_interval":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.DeltaFlushInterval = n

	case "marmot_vec_delta_max_rows":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.DeltaMaxRows = n

	case "marmot_vec_delta_flush_batch":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.DeltaFlushBatch = n

	case "marmot_vec_retrain_enabled":
		b, err := parseOnOff(name, value)
		if err != nil {
			return err
		}
		v.RetrainEnabled = b

	case "marmot_vec_retrain_check_interval":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.RetrainCheckInterval = n

	case "marmot_vec_retrain_growth_ratio":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("@@%s: invalid float %q: %w", name, value, err)
		}
		if f < 1.0 {
			return fmt.Errorf("@@%s: must be >= 1.0, got %v", name, f)
		}
		v.RetrainGrowthRatio = f

	case "marmot_vec_retrain_delta_ratio":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return fmt.Errorf("@@%s: invalid float %q: %w", name, value, err)
		}
		if f <= 0 || f > 1.0 {
			return fmt.Errorf("@@%s: must be in (0, 1.0], got %v", name, f)
		}
		v.RetrainDeltaRatio = f

	case "marmot_vec_reindex_chunk_rows":
		n, err := parsePositiveInt(name, value, false)
		if err != nil {
			return err
		}
		v.ReindexChunkRows = n

	default:
		return fmt.Errorf("unknown session variable @@%s", name)
	}
	return nil
}

// parseForcePlan converts "auto"/"pre"/"post" (case-insensitive) to ForcePlan.
// Returns MARMOT-VEC-012 for unrecognised values.
func parseForcePlan(s string) (ForcePlan, error) {
	switch strings.ToLower(s) {
	case "auto":
		return ForcePlanAuto, nil
	case "pre":
		return ForcePlanPre, nil
	case "post":
		return ForcePlanPost, nil
	default:
		return 0, fmt.Errorf("MARMOT-VEC-012: invalid @@marmot_vec_force_plan value %q: must be 'auto', 'pre', or 'post'", s)
	}
}

// parseOnOff converts "on"/"off" (case-insensitive) to bool.
func parseOnOff(name, s string) (bool, error) {
	switch strings.ToLower(s) {
	case "on":
		return true, nil
	case "off":
		return false, nil
	default:
		return false, fmt.Errorf("@@%s: invalid value %q: must be 'on' or 'off'", name, s)
	}
}

// parsePositiveInt parses a decimal integer from value.
// allowZero controls whether 0 is valid (e.g., nprobe=0 means "use index default").
func parsePositiveInt(name, value string, allowZero bool) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("@@%s: invalid integer %q: %w", name, value, err)
	}
	if allowZero && n < 0 {
		return 0, fmt.Errorf("@@%s: must be >= 0, got %d", name, n)
	}
	if !allowZero && n <= 0 {
		return 0, fmt.Errorf("@@%s: must be > 0, got %d", name, n)
	}
	return n, nil
}
