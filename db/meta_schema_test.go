package db

import (
	"fmt"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// TestStatementTypeToOpType verifies the mapping between protocol.StatementCode and OpType.
// This test is critical for preventing enum mismatch bugs where UPDATE was incorrectly
// mapped to DELETE due to off-by-one errors in magic number constants.
func TestStatementTypeToOpType(t *testing.T) {
	tests := []struct {
		name     string
		input    protocol.StatementCode
		expected OpType
	}{
		{
			name:     "Insert maps to OpTypeInsert",
			input:    protocol.StatementInsert,
			expected: OpTypeInsert,
		},
		{
			name:     "Replace maps to OpTypeReplace",
			input:    protocol.StatementReplace,
			expected: OpTypeReplace,
		},
		{
			name:     "Update maps to OpTypeUpdate",
			input:    protocol.StatementUpdate,
			expected: OpTypeUpdate,
		},
		{
			name:     "Delete maps to OpTypeDelete",
			input:    protocol.StatementDelete,
			expected: OpTypeDelete,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StatementTypeToOpType(tt.input)
			if got != tt.expected {
				t.Errorf("StatementTypeToOpType(%v) = %v (%s), want %v (%s)",
					tt.input, got, got.String(), tt.expected, tt.expected.String())
			}
		})
	}
}

// TestStatementTypeToOpTypePanicsOnUnsupported verifies that non-DML statement types panic.
func TestStatementTypeToOpTypePanicsOnUnsupported(t *testing.T) {
	unsupportedTypes := []protocol.StatementCode{
		protocol.StatementUnknown,
		protocol.StatementDDL,
		protocol.StatementCreateDatabase,
		protocol.StatementDropDatabase,
	}

	for _, stmtType := range unsupportedTypes {
		stmtType := stmtType // capture for closure
		t.Run(fmt.Sprintf("StatementCode_%d", stmtType), func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("StatementTypeToOpType(%d) should panic but didn't", stmtType)
				}
			}()
			StatementTypeToOpType(stmtType)
		})
	}
}

// TestStatementTypeEnumValues verifies the actual numeric values of protocol.StatementCode.
// This catches bugs where someone changes the enum order or adds new values in the middle.
func TestStatementTypeEnumValues(t *testing.T) {
	// These are the canonical values. If they change, something is wrong.
	if int(protocol.StatementUnknown) != 0 {
		t.Errorf("StatementUnknown should be 0, got %d", protocol.StatementUnknown)
	}
	if int(protocol.StatementInsert) != 1 {
		t.Errorf("StatementInsert should be 1, got %d", protocol.StatementInsert)
	}
	if int(protocol.StatementReplace) != 2 {
		t.Errorf("StatementReplace should be 2, got %d", protocol.StatementReplace)
	}
	if int(protocol.StatementUpdate) != 3 {
		t.Errorf("StatementUpdate should be 3, got %d", protocol.StatementUpdate)
	}
	if int(protocol.StatementDelete) != 4 {
		t.Errorf("StatementDelete should be 4, got %d", protocol.StatementDelete)
	}
}

// TestOpTypeEnumValues verifies the actual numeric values of OpType.
func TestOpTypeEnumValues(t *testing.T) {
	if int(OpTypeInsert) != 0 {
		t.Errorf("OpTypeInsert should be 0, got %d", OpTypeInsert)
	}
	if int(OpTypeReplace) != 1 {
		t.Errorf("OpTypeReplace should be 1, got %d", OpTypeReplace)
	}
	if int(OpTypeUpdate) != 2 {
		t.Errorf("OpTypeUpdate should be 2, got %d", OpTypeUpdate)
	}
	if int(OpTypeDelete) != 3 {
		t.Errorf("OpTypeDelete should be 3, got %d", OpTypeDelete)
	}
	if int(OpTypeDelta) != 4 {
		t.Errorf("OpTypeDelta should be 4, got %d", OpTypeDelta)
	}
}

// TestOpTypeString verifies String() method returns correct values.
func TestOpTypeString(t *testing.T) {
	tests := []struct {
		op   OpType
		want string
	}{
		{OpTypeInsert, "INSERT"},
		{OpTypeReplace, "REPLACE"},
		{OpTypeUpdate, "UPDATE"},
		{OpTypeDelete, "DELETE"},
		{OpTypeDelta, "DELTA"},
		{OpType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("OpType(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}

// TestIntentTypeString verifies IntentType String() method.
func TestIntentTypeString(t *testing.T) {
	tests := []struct {
		it   IntentType
		want string
	}{
		{IntentTypeDML, "DML"},
		{IntentTypeDDL, "DDL"},
		{IntentTypeDatabaseOp, "DATABASE_OP"},
		{IntentType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.it.String(); got != tt.want {
			t.Errorf("IntentType(%d).String() = %q, want %q", tt.it, got, tt.want)
		}
	}
}

// TestTxnStatusString verifies TxnStatus String() method.
func TestTxnStatusString(t *testing.T) {
	tests := []struct {
		ts   TxnStatus
		want string
	}{
		{TxnStatusPending, "PENDING"},
		{TxnStatusCommitted, "COMMITTED"},
		{TxnStatusAborted, "ABORTED"},
		{TxnStatus(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.ts.String(); got != tt.want {
			t.Errorf("TxnStatus(%d).String() = %q, want %q", tt.ts, got, tt.want)
		}
	}
}

// TestDatabaseOpTypeString verifies DatabaseOpType String() method.
func TestDatabaseOpTypeString(t *testing.T) {
	tests := []struct {
		op   DatabaseOpType
		want string
	}{
		{DatabaseOpCreate, "CREATE_DATABASE"},
		{DatabaseOpDrop, "DROP_DATABASE"},
		{DatabaseOpType(99), "UNKNOWN"},
	}

	for _, tt := range tests {
		if got := tt.op.String(); got != tt.want {
			t.Errorf("DatabaseOpType(%d).String() = %q, want %q", tt.op, got, tt.want)
		}
	}
}
