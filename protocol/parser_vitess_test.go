package protocol

import (
	"math"
	"strings"
	"testing"
)

func TestParseVectorIndexDDL_PipelineMetadata(t *testing.T) {
	t.Parallel()

	stmt := ParseStatement("CREATE VECTOR INDEX docs_embed_idx ON docs(embed) DIM 1536 METRIC cosine WITH (nlist = 8, nprobe = 4, max_norm = 1.5)")
	if stmt.Type != StatementCreateVectorIndex {
		t.Fatalf("Type = %v, want StatementCreateVectorIndex (err=%q)", stmt.Type, stmt.Error)
	}
	if stmt.VectorIndexName != "docs_embed_idx" {
		t.Fatalf("VectorIndexName = %q, want docs_embed_idx", stmt.VectorIndexName)
	}
	if stmt.TableName != "docs" {
		t.Fatalf("TableName = %q, want docs", stmt.TableName)
	}
	if stmt.VectorColumnName != "embed" {
		t.Fatalf("VectorColumnName = %q, want embed", stmt.VectorColumnName)
	}
	if stmt.VectorDim != 1536 || stmt.VectorMetric != "cosine" {
		t.Fatalf("vector shape = (%d,%q), want (1536,cosine)", stmt.VectorDim, stmt.VectorMetric)
	}
	if stmt.VectorNlist != 8 || stmt.VectorNprobe != 4 {
		t.Fatalf("probe config = (%d,%d), want (8,4)", stmt.VectorNlist, stmt.VectorNprobe)
	}
	if math.Abs(float64(stmt.VectorMaxNorm-1.5)) > 0.0001 {
		t.Fatalf("VectorMaxNorm = %f, want 1.5", stmt.VectorMaxNorm)
	}
}

func TestParseDropVectorIndexDDL_PipelineMetadata(t *testing.T) {
	t.Parallel()

	stmt := ParseStatement("DROP VECTOR INDEX docs_embed_idx ON docs")
	if stmt.Type != StatementDropVectorIndex {
		t.Fatalf("Type = %v, want StatementDropVectorIndex (err=%q)", stmt.Type, stmt.Error)
	}
	if stmt.VectorIndexName != "docs_embed_idx" {
		t.Fatalf("VectorIndexName = %q, want docs_embed_idx", stmt.VectorIndexName)
	}
	if stmt.TableName != "docs" {
		t.Fatalf("TableName = %q, want docs", stmt.TableName)
	}
}

func TestParseVectorQueryDoesNotExtractPlannerLiterals(t *testing.T) {
	t.Parallel()

	stmt := ParseStatementWithOptions(
		"SELECT id FROM docs WHERE vec_match(embed, X'01020304', 10) ORDER BY vec_distance(embed, X'01020304') LIMIT 10",
		ParseOptions{ExtractLiterals: true},
	)
	if stmt.Type != StatementSelect {
		t.Fatalf("Type = %v, want StatementSelect (err=%q)", stmt.Type, stmt.Error)
	}
	if len(stmt.ExtractedParams) != 0 {
		t.Fatalf("ExtractedParams length = %d, want 0", len(stmt.ExtractedParams))
	}
	if !strings.Contains(stmt.SQL, "vec_match(embed, X'01020304', 10)") {
		t.Fatalf("SQL = %q, want vec_match K literal preserved", stmt.SQL)
	}
}

// TestReindexVectorIndex_IsMutation guards the routing contract: REINDEX
// must flow through the same mutation path as other vector DDL so it's
// rejected during shutdown drain and replicated as vector-control metadata.
func TestReindexVectorIndex_IsMutation(t *testing.T) {
	t.Parallel()
	stmt := Statement{Type: StatementReindexVectorIndex}
	if !IsMutation(stmt) {
		t.Fatal("StatementReindexVectorIndex must be a mutation")
	}
}
