package query

import (
	"testing"
)

func TestPipelineAutoIncrementZeroTranspile(t *testing.T) {
	sql := "INSERT INTO sbtest1 (id, k, c, pad) VALUES (0, 1, 'test', 'pad')"

	ctx := NewContext(sql, nil)

	t.Logf("Source Dialect: %v", ctx.SourceDialect)
	t.Logf("NeedsTranspile: %v", ctx.NeedsTranspile)

	if ctx.SourceDialect != DialectMySQL {
		t.Errorf("Expected DialectMySQL, got %v", ctx.SourceDialect)
	}

	if !ctx.NeedsTranspile {
		t.Errorf("Expected NeedsTranspile to be true")
	}

	pipeline, err := NewPipeline(1000, 100)
	if err != nil {
		t.Fatalf("Failed to create pipeline: %v", err)
	}

	err = pipeline.Process(ctx)
	if err != nil {
		t.Fatalf("Pipeline error: %v", err)
	}

	t.Logf("Original SQL: %s", ctx.OriginalSQL)
	t.Logf("Transpiled SQL: %s", ctx.TranspiledSQL)
	t.Logf("Transformations: %+v", ctx.Transformations)

	// Check that id=0 was transformed to id=NULL
	expectedContains := "null"
	if ctx.TranspiledSQL == ctx.OriginalSQL {
		t.Errorf("SQL was not transpiled")
	}

	// The transpiled SQL should contain null instead of 0 for id
	if !containsIgnoreCase(ctx.TranspiledSQL, expectedContains) {
		t.Errorf("Transpiled SQL should contain 'null', got: %s", ctx.TranspiledSQL)
	}
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsIgnoreCase(s[1:], substr) || containsAt(s, substr))
}

func containsAt(s, substr string) bool {
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i < len(substr); i++ {
		sc := s[i]
		tc := substr[i]
		if sc >= 'A' && sc <= 'Z' {
			sc += 'a' - 'A'
		}
		if tc >= 'A' && tc <= 'Z' {
			tc += 'a' - 'A'
		}
		if sc != tc {
			return false
		}
	}
	return true
}
