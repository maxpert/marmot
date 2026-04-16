package query

import (
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"
)

// VarKind identifies the type of a session variable value.
type VarKind int

const (
	// VarKindInt indicates the variable holds an integer value.
	VarKindInt VarKind = iota + 1
	// VarKindEnum indicates the variable holds one of a fixed set of strings.
	VarKindEnum
)

// SessionVarSpec describes the validation rules for a single @@marmot_vec_*
// session variable.
type SessionVarSpec struct {
	Name    string
	Kind    VarKind
	Default any      // int64 for VarKindInt, string for VarKindEnum
	Enum    []string // valid values for VarKindEnum
	MinInt  int64    // inclusive lower bound for VarKindInt
}

// MarmotVecSessionVars is the catalog of supported @@marmot_vec_* session
// variables. Keys are the full variable names as presented in SET statements.
var MarmotVecSessionVars = map[string]SessionVarSpec{
	"marmot_vec_nprobe": {
		Name:    "marmot_vec_nprobe",
		Kind:    VarKindInt,
		Default: int64(16),
		MinInt:  1,
	},
	"marmot_vec_force_plan": {
		Name:    "marmot_vec_force_plan",
		Kind:    VarKindEnum,
		Default: "auto",
		Enum:    []string{"auto", "pre", "post"},
	},
	"marmot_vec_prefilter_cap": {
		Name:    "marmot_vec_prefilter_cap",
		Kind:    VarKindInt,
		Default: int64(5000),
		MinInt:  1,
	},
	"marmot_vec_fallback": {
		Name:    "marmot_vec_fallback",
		Kind:    VarKindEnum,
		Default: "on",
		Enum:    []string{"on", "off"},
	},
}

// ValidateMarmotVecSessionVar validates a SET @@<name> = <value> against the
// catalog. value must be a Vitess AST Expr (typically *sqlparser.Literal).
// Returns the normalised value (int64 or string) or a MARMOT-VEC-012 error.
// No string matching is performed on rendered SQL; the AST node is inspected
// directly.
func ValidateMarmotVecSessionVar(name string, value sqlparser.Expr) (any, error) {
	spec, ok := MarmotVecSessionVars[name]
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-012: unknown session variable %q", name)
	}

	lit, ok := value.(*sqlparser.Literal)
	if !ok {
		return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q requires a literal value", name)
	}

	switch spec.Kind {
	case VarKindInt:
		// Vitess represents integer literals as IntVal type.
		if lit.Type != sqlparser.IntVal {
			return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q requires an integer value", name)
		}
		n, err := strconv.ParseInt(lit.Val, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q value %q is not a valid integer", name, lit.Val)
		}
		if n < spec.MinInt {
			return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q value %d is below minimum %d", name, n, spec.MinInt)
		}
		return n, nil

	case VarKindEnum:
		// Vitess represents quoted string literals as StrVal type.
		if lit.Type != sqlparser.StrVal {
			return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q requires a string value", name)
		}
		for _, valid := range spec.Enum {
			if lit.Val == valid {
				return lit.Val, nil
			}
		}
		return nil, fmt.Errorf("MARMOT-VEC-012: session variable %q value %q is not valid; allowed: %v",
			name, lit.Val, spec.Enum)

	default:
		return nil, fmt.Errorf("MARMOT-VEC-012: unknown variable kind for %q", name)
	}
}
