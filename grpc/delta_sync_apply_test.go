package grpc

import (
	"context"
	"os"
	"testing"

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
					RowChange: &RowChange{
						NewValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "alice"),
						},
					},
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
					RowChange: &RowChange{
						OldValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "alice"),
						},
						NewValues: map[string][]byte{
							"id":   mustMarshalMsgpack(t, int64(1)),
							"name": mustMarshalMsgpack(t, "bob"),
						},
					},
				},
			},
		},
	}
	if err := ds.applyChangeEvent(context.Background(), updateEvent); err != nil {
		t.Fatalf("applyChangeEvent UPDATE after DDL should succeed, got: %v", err)
	}
}
