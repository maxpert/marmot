package protocol

import (
	"math"
	"strings"
	"testing"
)

// TestParseReindexVectorDDL_Positive covers the happy path plus common
// acceptable formatting (trailing semicolon, extra whitespace, mixed case).
// Locks the contract described in design §8.3: `REINDEX VECTOR <name>` is
// a SQLite-specific DDL routed to VectorIndexManager.ReindexIndex.
func TestParseReindexVectorDDL_Positive(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sql  string
		want string
	}{
		{"REINDEX VECTOR embeddings", "embeddings"},
		{"reindex vector embeddings", "embeddings"},
		{"  REINDEX   VECTOR   my_idx  ", "my_idx"},
		{"REINDEX VECTOR embeddings;", "embeddings"},
		{"REINDEX VECTOR embeddings ;", "embeddings"},
		{"/* rebuild */ REINDEX VECTOR embeddings", "embeddings"},
		{"REINDEX VECTOR `my_idx`", "my_idx"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.sql, func(t *testing.T) {
			stmt := ParseStatementVitess(tc.sql)
			if stmt.Type != StatementReindexVectorIndex {
				t.Fatalf("Type = %v, want StatementReindexVectorIndex (err=%q)", stmt.Type, stmt.Error)
			}
			if stmt.VectorIndexName != tc.want {
				t.Fatalf("VectorIndexName = %q, want %q", stmt.VectorIndexName, tc.want)
			}
		})
	}
}

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

// TestParseReindexVectorDDL_Negative covers rejection paths and passthrough.
// `REINDEX TABLE foo` and plain `REINDEX` must NOT be claimed by the
// VectorReindex dispatcher — they are legitimate SQLite statements that
// belong to the standard execution path.
func TestParseReindexVectorDDL_Negative(t *testing.T) {
	t.Parallel()

	t.Run("missing index name", func(t *testing.T) {
		stmt := ParseStatementVitess("REINDEX VECTOR")
		if stmt.Type != StatementUnsupported {
			t.Fatalf("Type = %v, want StatementUnsupported", stmt.Type)
		}
		if !strings.Contains(stmt.Error, "MARMOT-VEC-015") {
			t.Fatalf("Error = %q, want MARMOT-VEC-015 prefix", stmt.Error)
		}
	})

	t.Run("extra tokens", func(t *testing.T) {
		stmt := ParseStatementVitess("REINDEX VECTOR foo bar")
		if stmt.Type != StatementUnsupported {
			t.Fatalf("Type = %v, want StatementUnsupported", stmt.Type)
		}
		if !strings.Contains(stmt.Error, "MARMOT-VEC-015") {
			t.Fatalf("Error = %q, want MARMOT-VEC-015 prefix", stmt.Error)
		}
	})

	t.Run("REINDEX TABLE is not claimed", func(t *testing.T) {
		// `REINDEX TABLE foo` is a legitimate SQLite REINDEX — must NOT be
		// intercepted as vector reindex.
		stmt := ParseStatementVitess("REINDEX TABLE foo")
		if stmt.Type == StatementReindexVectorIndex {
			t.Fatalf("REINDEX TABLE foo must not be classified as StatementReindexVectorIndex")
		}
	})

	t.Run("plain REINDEX is not claimed", func(t *testing.T) {
		stmt := ParseStatementVitess("REINDEX")
		if stmt.Type == StatementReindexVectorIndex {
			t.Fatalf("plain REINDEX must not be classified as StatementReindexVectorIndex")
		}
	})

	t.Run("unrelated statement is not claimed", func(t *testing.T) {
		stmt := ParseStatementVitess("SELECT 1")
		if stmt.Type == StatementReindexVectorIndex {
			t.Fatalf("SELECT must not be classified as StatementReindexVectorIndex")
		}
	})
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
