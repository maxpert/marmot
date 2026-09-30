package query

import (
	"testing"

	"github.com/maxpert/marmot/protocol/query/transform"
)

// ruleErrorSchemas gives the target table an auto-increment column, which is
// what makes an INSERT ... SELECT that does not project it a rejection.
var ruleErrorSchemas = map[string]transform.SchemaInfo{
	"orders": {AutoIncrementColumn: "id", AutoIncrementOrdinal: 0},
}

// TestTranspileReturnsRuleErrorVerbatim is the regression guard on the
// swallow that used to sit in Transpile: `if err == nil && applied`, which
// dropped every error a rule returned and let the statement proceed.
func TestTranspileReturnsRuleErrorVerbatim(t *testing.T) {
	pipeline, err := NewPipeline(1000, &mockIDGenerator{})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx := NewContext("INSERT INTO orders SELECT id, total FROM staging", nil)
	ctx.SchemaLookup = mockSchemaLookup(ruleErrorSchemas)

	if err := pipeline.parser.Parse(ctx); err != nil {
		t.Fatalf("parse: %v", err)
	}

	gotErr := pipeline.transpiler.Transpile(ctx)

	// Mutation: restore `if err == nil && applied` in Transpile. gotErr is nil
	// and this fires - the statement would have gone on to SQLite, which
	// assigns the rowid itself.
	if gotErr == nil {
		t.Fatal("Transpile returned nil; the rule's rejection was swallowed")
	}

	// Verbatim: a direct type assertion, not errors.As, so wrapping the error
	// on the way out also fails this.
	// Mutation: wrap the error in Transpile with fmt.Errorf("%w").
	coded, ok := gotErr.(*transform.CodedError)
	if !ok {
		t.Fatalf("Transpile returned %T (%v); want the rule's own *transform.CodedError", gotErr, gotErr)
	}
	if coded.Code != transform.ErrCodeNotSupportedYet {
		t.Errorf("error code = %d, want %d", coded.Code, transform.ErrCodeNotSupportedYet)
	}
}

// TestProcessRecordsTranspileErrorTyped pins that the pipeline hands the typed
// error up to the protocol layer. Without it the protocol layer cannot tell a
// rule's rejection from a Vitess syntax error and reports 1064 for both.
func TestProcessRecordsTranspileErrorTyped(t *testing.T) {
	pipeline, err := NewPipeline(1000, &mockIDGenerator{})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx := NewContext("INSERT INTO orders SELECT id, total FROM staging", nil)
	ctx.SchemaLookup = mockSchemaLookup(ruleErrorSchemas)

	if err := pipeline.Process(ctx); err == nil {
		t.Fatal("Process returned nil for a statement the rule refuses")
	}

	// Mutation: delete the `ctx.Output.TranspileErr = err` assignment in
	// Pipeline.Process.
	if ctx.Output.TranspileErr == nil {
		t.Fatal("Process did not record TranspileErr")
	}
	if _, ok := ctx.Output.TranspileErr.(*transform.CodedError); !ok {
		t.Errorf("TranspileErr is %T, want *transform.CodedError", ctx.Output.TranspileErr)
	}
}

// TestProcessLeavesTranspileErrNilOnParseFailure is the converse guard: a
// syntax error must NOT be reported as a rule rejection, or every parse error
// changes error code.
// Mutation: set ctx.Output.TranspileErr in the parser branch of Process too.
func TestProcessLeavesTranspileErrNilOnParseFailure(t *testing.T) {
	pipeline, err := NewPipeline(1000, &mockIDGenerator{})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}

	ctx := NewContext("SELECT FROM WHERE ORDER BY", nil)
	if err := pipeline.Process(ctx); err == nil {
		t.Fatal("Process accepted a syntactically invalid statement")
	}
	if ctx.Output.TranspileErr != nil {
		t.Errorf("TranspileErr = %v for a parse failure; want nil", ctx.Output.TranspileErr)
	}
}

// TestTranspileRefusesAnAlterFloorNoColumnTakes pins that the ALTER refusal
// reaches the protocol layer as ER_NOT_SUPPORTED_YET through the real rule set,
// rather than the floor being dropped or the statement reaching SQLite, which
// would report a syntax error (1064).
func TestTranspileRefusesAnAlterFloorNoColumnTakes(t *testing.T) {
	pipeline, err := NewPipeline(1000, &mockIDGenerator{})
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	for _, sql := range []string{
		"ALTER TABLE orders AUTO_INCREMENT=5000",
		"ALTER TABLE orders ADD COLUMN note TEXT, AUTO_INCREMENT=5000",
	} {
		ctx := NewContext(sql, nil)
		ctx.SchemaLookup = mockSchemaLookup(ruleErrorSchemas)
		if err := pipeline.parser.Parse(ctx); err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		coded, ok := pipeline.transpiler.Transpile(ctx).(*transform.CodedError)
		if !ok || coded.Code != transform.ErrCodeNotSupportedYet {
			t.Errorf("%q: Transpile did not refuse with ER_NOT_SUPPORTED_YET", sql)
		}
	}
}
