package grpc

import "github.com/maxpert/marmot/protocol"

// Maps for bidirectional conversion - single source of truth
var (
	statementCodeToProto = map[protocol.StatementCode]StatementType{
		protocol.StatementInsert:         StatementType_INSERT,
		protocol.StatementUpdate:         StatementType_UPDATE,
		protocol.StatementDelete:         StatementType_DELETE,
		protocol.StatementReplace:        StatementType_REPLACE,
		protocol.StatementDDL:            StatementType_DDL,
		protocol.StatementCreateDatabase: StatementType_CREATE_DATABASE,
		protocol.StatementDropDatabase:   StatementType_DROP_DATABASE,
	}

	protoToStatementCode = map[StatementType]protocol.StatementCode{
		StatementType_INSERT:          protocol.StatementInsert,
		StatementType_UPDATE:          protocol.StatementUpdate,
		StatementType_DELETE:          protocol.StatementDelete,
		StatementType_REPLACE:         protocol.StatementReplace,
		StatementType_DDL:             protocol.StatementDDL,
		StatementType_CREATE_DATABASE: protocol.StatementCreateDatabase,
		StatementType_DROP_DATABASE:   protocol.StatementDropDatabase,
	}
)

// ToProtoStatementType converts protocol.StatementCode to gRPC StatementType
func ToProtoStatementType(code protocol.StatementCode) (StatementType, bool) {
	st, ok := statementCodeToProto[code]
	return st, ok
}

// FromProtoStatementType converts gRPC StatementType to protocol.StatementCode
func FromProtoStatementType(st StatementType) (protocol.StatementCode, bool) {
	code, ok := protoToStatementCode[st]
	return code, ok
}

// MustFromProtoStatementType converts gRPC StatementType to protocol.StatementCode with default fallback
func MustFromProtoStatementType(st StatementType) protocol.StatementCode {
	if code, ok := protoToStatementCode[st]; ok {
		return code
	}
	return protocol.StatementInsert // default for unknown
}
