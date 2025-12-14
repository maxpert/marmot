package grpc

import (
	"testing"

	"github.com/maxpert/marmot/protocol"
	"github.com/stretchr/testify/assert"
)

func TestStatementCodeMapConsistency(t *testing.T) {
	// Verify forward and reverse maps are consistent
	for code, proto := range statementCodeToProto {
		gotCode, ok := protoToStatementCode[proto]
		assert.True(t, ok, "proto %v not in reverse map", proto)
		assert.Equal(t, code, gotCode)
	}
	for proto, code := range protoToStatementCode {
		gotProto, ok := statementCodeToProto[code]
		assert.True(t, ok, "code %v not in forward map", code)
		assert.Equal(t, proto, gotProto)
	}
}

func TestAllWireTypesAreMapped(t *testing.T) {
	// Ensure all wire types have mappings
	wireTypes := []StatementType{
		StatementType_INSERT,
		StatementType_UPDATE,
		StatementType_DELETE,
		StatementType_REPLACE,
		StatementType_DDL,
		StatementType_CREATE_DATABASE,
		StatementType_DROP_DATABASE,
	}
	for _, st := range wireTypes {
		_, ok := protoToStatementCode[st]
		assert.True(t, ok, "wire type %v has no mapping", st)
	}
}

func TestToProtoStatementType(t *testing.T) {
	tests := []struct {
		name   string
		code   protocol.StatementCode
		want   StatementType
		wantOk bool
	}{
		{"INSERT", protocol.StatementInsert, StatementType_INSERT, true},
		{"UPDATE", protocol.StatementUpdate, StatementType_UPDATE, true},
		{"DELETE", protocol.StatementDelete, StatementType_DELETE, true},
		{"REPLACE", protocol.StatementReplace, StatementType_REPLACE, true},
		{"DDL", protocol.StatementDDL, StatementType_DDL, true},
		{"CREATE_DATABASE", protocol.StatementCreateDatabase, StatementType_CREATE_DATABASE, true},
		{"DROP_DATABASE", protocol.StatementDropDatabase, StatementType_DROP_DATABASE, true},
		{"UNKNOWN", protocol.StatementUnknown, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToProtoStatementType(tt.code)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestFromProtoStatementType(t *testing.T) {
	tests := []struct {
		name   string
		proto  StatementType
		want   protocol.StatementCode
		wantOk bool
	}{
		{"INSERT", StatementType_INSERT, protocol.StatementInsert, true},
		{"UPDATE", StatementType_UPDATE, protocol.StatementUpdate, true},
		{"DELETE", StatementType_DELETE, protocol.StatementDelete, true},
		{"REPLACE", StatementType_REPLACE, protocol.StatementReplace, true},
		{"DDL", StatementType_DDL, protocol.StatementDDL, true},
		{"CREATE_DATABASE", StatementType_CREATE_DATABASE, protocol.StatementCreateDatabase, true},
		{"DROP_DATABASE", StatementType_DROP_DATABASE, protocol.StatementDropDatabase, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromProtoStatementType(tt.proto)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestMustFromProtoStatementType(t *testing.T) {
	tests := []struct {
		name  string
		proto StatementType
		want  protocol.StatementCode
	}{
		{"INSERT", StatementType_INSERT, protocol.StatementInsert},
		{"UPDATE", StatementType_UPDATE, protocol.StatementUpdate},
		{"DELETE", StatementType_DELETE, protocol.StatementDelete},
		{"REPLACE", StatementType_REPLACE, protocol.StatementReplace},
		{"DDL", StatementType_DDL, protocol.StatementDDL},
		{"CREATE_DATABASE", StatementType_CREATE_DATABASE, protocol.StatementCreateDatabase},
		{"DROP_DATABASE", StatementType_DROP_DATABASE, protocol.StatementDropDatabase},
		{"UNKNOWN", StatementType(999), protocol.StatementInsert}, // fallback to INSERT
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MustFromProtoStatementType(tt.proto)
			assert.Equal(t, tt.want, got)
		})
	}
}
