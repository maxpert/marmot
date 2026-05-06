package grpc

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/db"
	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/maxpert/marmot/hlc"
)

func TestDeltaSyncApplyChangeEvent_DDLIdempotent(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_delta_apply_ddl_idempotent")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	const testDB = "test_delta_ddl"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	ds := NewDeltaSyncClient(DeltaSyncConfig{
		NodeID:           1,
		DBManager:        dbMgr,
		Clock:            hlc.NewClock(1),
		SchemaVersionMgr: schemaVersionMgr,
	})

	event := &ChangeEvent{
		TxnId:    300,
		Database: testDB,
		Timestamp: &HLC{
			WallTime: 1,
			Logical:  1,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:     pb.StatementType_DDL,
				Database: testDB,
				Payload: &Statement_DdlChange{
					DdlChange: &DDLChange{
						Sql: "CREATE TABLE delta_users (id INTEGER PRIMARY KEY, name TEXT)",
					},
				},
			},
		},
	}

	for i := 0; i < 2; i++ {
		if err := ds.applyChangeEvent(context.Background(), event); err != nil {
			t.Fatalf("applyChangeEvent call %d should be idempotent, got error: %v", i+1, err)
		}
	}
}

func TestDeltaSyncApplyChangeEvent_ReloadsSchemaAfterDDL(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_delta_apply_schema_reload")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	const testDB = "test_delta_schema"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}

	ds := NewDeltaSyncClient(DeltaSyncConfig{
		NodeID:           1,
		DBManager:        dbMgr,
		Clock:            hlc.NewClock(1),
		SchemaVersionMgr: schemaVersionMgr,
	})

	ddlEvent := &ChangeEvent{
		TxnId:    400,
		Database: testDB,
		Timestamp: &HLC{
			WallTime: 1,
			Logical:  1,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_DDL,
				TableName: "delta_updates",
				Database:  testDB,
				Payload: &Statement_DdlChange{
					DdlChange: &DDLChange{
						Sql: "CREATE TABLE delta_updates (id INTEGER PRIMARY KEY, name TEXT)",
					},
				},
			},
		},
	}
	if err := ds.applyChangeEvent(context.Background(), ddlEvent); err != nil {
		t.Fatalf("applyChangeEvent DDL failed: %v", err)
	}

	insertEvent := &ChangeEvent{
		TxnId:    401,
		Database: testDB,
		Timestamp: &HLC{
			WallTime: 2,
			Logical:  1,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_INSERT,
				TableName: "delta_updates",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: testInsertRowChange("delta_updates", []byte("delta_updates:1"), map[string][]byte{
						"id":   mustMarshalMsgpack(t, int64(1)),
						"name": mustMarshalMsgpack(t, "alice"),
					}),
				},
			},
		},
	}
	if err := ds.applyChangeEvent(context.Background(), insertEvent); err != nil {
		t.Fatalf("applyChangeEvent INSERT failed: %v", err)
	}

	updateEvent := &ChangeEvent{
		TxnId:    402,
		Database: testDB,
		Timestamp: &HLC{
			WallTime: 3,
			Logical:  1,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_UPDATE,
				TableName: "delta_updates",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: testUpdateRowChange("delta_updates", []byte("delta_updates:1"), map[string][]byte{
						"id":   mustMarshalMsgpack(t, int64(1)),
						"name": mustMarshalMsgpack(t, "alice"),
					}, map[string][]byte{
						"id":   mustMarshalMsgpack(t, int64(1)),
						"name": mustMarshalMsgpack(t, "bob"),
					}),
				},
			},
		},
	}
	if err := ds.applyChangeEvent(context.Background(), updateEvent); err != nil {
		t.Fatalf("applyChangeEvent UPDATE after DDL should succeed, got: %v", err)
	}
}

func TestDeltaSyncApplyChangeEvent_VectorCDCFailureDoesNotBlockRows(t *testing.T) {
	tmpDir, dbMgr, schemaVersionMgr := setupTestEnvironment(t, "test_delta_vector_dirty")
	defer os.RemoveAll(tmpDir)
	defer dbMgr.Close()

	const testDB = "test_delta_vector"
	if err := dbMgr.CreateDatabase(testDB); err != nil {
		t.Fatalf("Failed to create test database: %v", err)
	}
	mdb, err := dbMgr.GetDatabase(testDB)
	if err != nil {
		t.Fatalf("Failed to get test database: %v", err)
	}
	if _, err := mdb.GetDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, embed BLOB, title TEXT)`); err != nil {
		t.Fatalf("Failed to create docs: %v", err)
	}
	if err := mdb.ReloadSchema(); err != nil {
		t.Fatalf("ReloadSchema: %v", err)
	}

	vecMgr := db.NewVectorIndexManager(dbMgr)
	vecMgr.SetLifecycleHook(focusedVectorHook{err: errors.New("overlay write failed")})
	dbMgr.SetVectorIndexManager(vecMgr)
	if err := vecMgr.ApplyVectorControl(context.Background(), common.VectorIndexChange{
		Action:              common.VectorIndexActionCreate,
		Database:            testDB,
		IndexName:           "docs_embed_idx",
		TableName:           "docs",
		ColumnName:          "embed",
		Metric:              "cosine",
		Dim:                 4,
		Nlist:               8,
		Nprobe:              8,
		TargetPartitionSize: 512,
		CreatedAt:           time.Now().UnixNano(),
	}); err != nil {
		t.Fatalf("ApplyVectorControl: %v", err)
	}

	ds := NewDeltaSyncClient(DeltaSyncConfig{
		NodeID:           1,
		DBManager:        dbMgr,
		Clock:            hlc.NewClock(1),
		SchemaVersionMgr: schemaVersionMgr,
	})

	event := &ChangeEvent{
		TxnId:    403,
		Database: testDB,
		Timestamp: &HLC{
			WallTime: 4,
			Logical:  1,
			NodeId:   2,
		},
		Statements: []*Statement{
			{
				Type:      pb.StatementType_INSERT,
				TableName: "docs",
				Database:  testDB,
				Payload: &Statement_RowChange{
					RowChange: testInsertRowChange("docs", []byte("docs:1"), map[string][]byte{
						"id":    mustMarshalMsgpack(t, int64(1)),
						"embed": mustMarshalMsgpack(t, []byte{1, 2, 3, 4}),
						"title": mustMarshalMsgpack(t, "delta vector row"),
					}),
				},
			},
		},
	}

	if err := ds.applyChangeEvent(context.Background(), event); err != nil {
		t.Fatalf("vector CDC failure should not block delta row replication: %v", err)
	}
	var title string
	if err := mdb.GetDB().QueryRow(`SELECT title FROM docs WHERE id = 1`).Scan(&title); err != nil {
		t.Fatalf("Failed to query delta row: %v", err)
	}
	if title != "delta vector row" {
		t.Fatalf("title=%q, want delta vector row", title)
	}
	var status string
	if err := mdb.GetDB().QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`,
		"docs_embed_idx",
	).Scan(&status); err != nil {
		t.Fatalf("Failed to read vector status: %v", err)
	}
	if status != "dirty" {
		t.Fatalf("vector status=%q, want dirty", status)
	}
}
