package common

import (
	"testing"

	pb "github.com/maxpert/marmot/grpc/common"
	"github.com/stretchr/testify/assert"
)

func TestCodeToWireMapConsistency(t *testing.T) {
	// Verify forward and reverse maps are consistent
	for code, wire := range codeToWire {
		gotCode, ok := wireToCode[wire]
		assert.True(t, ok, "wire %v not in reverse map", wire)
		assert.Equal(t, code, gotCode)
	}
	for wire, code := range wireToCode {
		gotWire, ok := codeToWire[code]
		assert.True(t, ok, "code %v not in forward map", code)
		assert.Equal(t, wire, gotWire)
	}
}

func TestAllWireTypesAreMapped(t *testing.T) {
	wireTypes := []pb.StatementType{
		pb.StatementType_INSERT,
		pb.StatementType_UPDATE,
		pb.StatementType_DELETE,
		pb.StatementType_REPLACE,
		pb.StatementType_DDL,
		pb.StatementType_CREATE_DATABASE,
		pb.StatementType_DROP_DATABASE,
		pb.StatementType_LOAD_DATA,
	}
	for _, st := range wireTypes {
		_, ok := wireToCode[st]
		assert.True(t, ok, "wire type %v has no mapping", st)
	}
}

func TestToWireType(t *testing.T) {
	tests := []struct {
		name   string
		code   StatementCode
		want   pb.StatementType
		wantOk bool
	}{
		{"INSERT", StatementInsert, pb.StatementType_INSERT, true},
		{"UPDATE", StatementUpdate, pb.StatementType_UPDATE, true},
		{"DELETE", StatementDelete, pb.StatementType_DELETE, true},
		{"REPLACE", StatementReplace, pb.StatementType_REPLACE, true},
		{"DDL", StatementDDL, pb.StatementType_DDL, true},
		{"CREATE_DATABASE", StatementCreateDatabase, pb.StatementType_CREATE_DATABASE, true},
		{"DROP_DATABASE", StatementDropDatabase, pb.StatementType_DROP_DATABASE, true},
		{"LOAD_DATA", StatementLoadData, pb.StatementType_LOAD_DATA, true},
		{"UNKNOWN", StatementUnknown, 0, false},
		{"SELECT", StatementSelect, 0, false}, // SELECT has no wire representation
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ToWireType(tt.code)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestFromWireType(t *testing.T) {
	tests := []struct {
		name   string
		wire   pb.StatementType
		want   StatementCode
		wantOk bool
	}{
		{"INSERT", pb.StatementType_INSERT, StatementInsert, true},
		{"UPDATE", pb.StatementType_UPDATE, StatementUpdate, true},
		{"DELETE", pb.StatementType_DELETE, StatementDelete, true},
		{"REPLACE", pb.StatementType_REPLACE, StatementReplace, true},
		{"DDL", pb.StatementType_DDL, StatementDDL, true},
		{"CREATE_DATABASE", pb.StatementType_CREATE_DATABASE, StatementCreateDatabase, true},
		{"DROP_DATABASE", pb.StatementType_DROP_DATABASE, StatementDropDatabase, true},
		{"LOAD_DATA", pb.StatementType_LOAD_DATA, StatementLoadData, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := FromWireType(tt.wire)
			assert.Equal(t, tt.wantOk, ok)
			if tt.wantOk {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestMustToWireTypePanicsOnUnknown(t *testing.T) {
	assert.Panics(t, func() {
		MustToWireType(StatementSelect) // SELECT has no wire representation
	}, "MustToWireType should panic on code without wire representation")
}

func TestMustFromWireTypePanicsOnUnknown(t *testing.T) {
	assert.Panics(t, func() {
		MustFromWireType(pb.StatementType(999))
	}, "MustFromWireType should panic on unknown wire type")
}

func TestMustToWireType(t *testing.T) {
	tests := []struct {
		name string
		code StatementCode
		want pb.StatementType
	}{
		{"INSERT", StatementInsert, pb.StatementType_INSERT},
		{"UPDATE", StatementUpdate, pb.StatementType_UPDATE},
		{"DELETE", StatementDelete, pb.StatementType_DELETE},
		{"REPLACE", StatementReplace, pb.StatementType_REPLACE},
		{"DDL", StatementDDL, pb.StatementType_DDL},
		{"CREATE_DATABASE", StatementCreateDatabase, pb.StatementType_CREATE_DATABASE},
		{"DROP_DATABASE", StatementDropDatabase, pb.StatementType_DROP_DATABASE},
		{"LOAD_DATA", StatementLoadData, pb.StatementType_LOAD_DATA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MustToWireType(tt.code)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMustFromWireType(t *testing.T) {
	tests := []struct {
		name string
		wire pb.StatementType
		want StatementCode
	}{
		{"INSERT", pb.StatementType_INSERT, StatementInsert},
		{"UPDATE", pb.StatementType_UPDATE, StatementUpdate},
		{"DELETE", pb.StatementType_DELETE, StatementDelete},
		{"REPLACE", pb.StatementType_REPLACE, StatementReplace},
		{"DDL", pb.StatementType_DDL, StatementDDL},
		{"CREATE_DATABASE", pb.StatementType_CREATE_DATABASE, StatementCreateDatabase},
		{"DROP_DATABASE", pb.StatementType_DROP_DATABASE, StatementDropDatabase},
		{"LOAD_DATA", pb.StatementType_LOAD_DATA, StatementLoadData},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MustFromWireType(tt.wire)
			assert.Equal(t, tt.want, got)
		})
	}
}
