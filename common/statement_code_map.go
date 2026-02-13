// HARD RULE: ALL conversions between StatementCode and grpc StatementType
// MUST go through this file. NO EXCEPTIONS. NO direct int casts.
// This is the ONLY boundary for type conversions.
package common

import (
	"fmt"

	pb "github.com/maxpert/marmot/grpc/common"
)

// =============================================================================
// StatementCode <-> grpc/common.StatementType (wire boundary)
// =============================================================================

var (
	// Internal StatementCode -> Wire StatementType
	codeToWire = map[StatementCode]pb.StatementType{
		StatementInsert:         pb.StatementType_INSERT,
		StatementUpdate:         pb.StatementType_UPDATE,
		StatementDelete:         pb.StatementType_DELETE,
		StatementReplace:        pb.StatementType_REPLACE,
		StatementDDL:            pb.StatementType_DDL,
		StatementCreateDatabase: pb.StatementType_CREATE_DATABASE,
		StatementDropDatabase:   pb.StatementType_DROP_DATABASE,
		StatementLoadData:       pb.StatementType_LOAD_DATA,
	}

	// Wire StatementType -> Internal StatementCode
	wireToCode = map[pb.StatementType]StatementCode{
		pb.StatementType_INSERT:          StatementInsert,
		pb.StatementType_UPDATE:          StatementUpdate,
		pb.StatementType_DELETE:          StatementDelete,
		pb.StatementType_REPLACE:         StatementReplace,
		pb.StatementType_DDL:             StatementDDL,
		pb.StatementType_CREATE_DATABASE: StatementCreateDatabase,
		pb.StatementType_DROP_DATABASE:   StatementDropDatabase,
		pb.StatementType_LOAD_DATA:       StatementLoadData,
	}
)

// ToWireType converts internal StatementCode to wire StatementType.
// Returns false if the code has no wire representation.
func ToWireType(code StatementCode) (pb.StatementType, bool) {
	st, ok := codeToWire[code]
	return st, ok
}

// FromWireType converts wire StatementType to internal StatementCode.
// Returns false if the wire type is unknown.
func FromWireType(st pb.StatementType) (StatementCode, bool) {
	code, ok := wireToCode[st]
	return code, ok
}

// MustToWireType converts internal StatementCode to wire StatementType.
// Panics on unknown type - use at gRPC send boundary.
func MustToWireType(code StatementCode) pb.StatementType {
	if st, ok := codeToWire[code]; ok {
		return st
	}
	panic(fmt.Sprintf("unknown StatementCode %d: cannot convert to wire type", code))
}

// MustFromWireType converts wire StatementType to internal StatementCode.
// Panics on unknown type - use at gRPC receive boundary where invalid data is unacceptable.
func MustFromWireType(st pb.StatementType) StatementCode {
	if code, ok := wireToCode[st]; ok {
		return code
	}
	panic(fmt.Sprintf("unknown wire StatementType %d: cannot convert to internal code", st))
}
