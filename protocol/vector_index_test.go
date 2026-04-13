package protocol

import (
	"testing"

	"github.com/maxpert/marmot/common"
)

func TestVectorIndex_CreateParses(t *testing.T) {
	stmt := ParseStatement("CREATE VECTOR INDEX idx_embed ON articles(embedding) WITH (metric='cosine', dim=768)")
	if stmt.Type != common.StatementCreateVectorIndex {
		t.Errorf("expected StatementCreateVectorIndex, got %d", stmt.Type)
	}
	if stmt.TableName != "articles" {
		t.Errorf("expected table 'articles', got %q", stmt.TableName)
	}
}

func TestVectorIndex_CreateIfNotExists(t *testing.T) {
	stmt := ParseStatement("CREATE VECTOR INDEX IF NOT EXISTS idx ON t(col) WITH (metric='euclidean', dim=128)")
	if stmt.Type != common.StatementCreateVectorIndex {
		t.Errorf("expected StatementCreateVectorIndex, got %d", stmt.Type)
	}
}

func TestVectorIndex_DropParses(t *testing.T) {
	stmt := ParseStatement("DROP VECTOR INDEX idx_embed ON articles")
	if stmt.Type != common.StatementDropVectorIndex {
		t.Errorf("expected StatementDropVectorIndex, got %d", stmt.Type)
	}
	if stmt.TableName != "articles" {
		t.Errorf("expected table 'articles', got %q", stmt.TableName)
	}
}

func TestVectorIndex_DropIfExists(t *testing.T) {
	stmt := ParseStatement("DROP VECTOR INDEX IF EXISTS idx ON t")
	if stmt.Type != common.StatementDropVectorIndex {
		t.Errorf("expected StatementDropVectorIndex, got %d", stmt.Type)
	}
}

func TestVectorIndex_IsMutation(t *testing.T) {
	if !common.StatementCreateVectorIndex.IsMutation() {
		t.Error("StatementCreateVectorIndex should be a mutation")
	}
	if !common.StatementDropVectorIndex.IsMutation() {
		t.Error("StatementDropVectorIndex should be a mutation")
	}
}

func TestVectorIndex_RegularDDL_Unchanged(t *testing.T) {
	stmt := ParseStatement("CREATE TABLE foo (id INTEGER PRIMARY KEY)")
	if stmt.Type != common.StatementDDL {
		t.Errorf("regular CREATE TABLE should be StatementDDL, got %d", stmt.Type)
	}
}

func TestVectorIndex_RegularDropIndex_Unchanged(t *testing.T) {
	stmt := ParseStatement("DROP INDEX idx_name")
	if stmt.Type != common.StatementDDL {
		t.Errorf("regular DROP INDEX should be StatementDDL, got %d", stmt.Type)
	}
}
