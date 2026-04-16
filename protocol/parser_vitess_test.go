package protocol

import (
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
// rejected during shutdown drain and routed to handleVectorDDL.
func TestReindexVectorIndex_IsMutation(t *testing.T) {
	t.Parallel()
	stmt := Statement{Type: StatementReindexVectorIndex}
	if !IsMutation(stmt) {
		t.Fatal("StatementReindexVectorIndex must be a mutation")
	}
}
