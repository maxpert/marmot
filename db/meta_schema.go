package db

// Transaction status constants
const (
	TxnStatusPending   = "PENDING"
	TxnStatusCommitted = "COMMITTED"
	TxnStatusAborted   = "ABORTED"
)

// Operation type constants (string form)
const (
	OpInsert = "INSERT"
	OpUpdate = "UPDATE"
	OpDelete = "DELETE"
)

// Operation constants for intent entries (uint8 form)
const (
	OpInsertInt uint8 = 0
	OpUpdateInt uint8 = 1
	OpDeleteInt uint8 = 2
)

// Internal table names for special operations
const (
	TableDatabaseOperations = "__marmot__database_operations"
	TableDDLOps             = "__marmot__ddl_ops"
)

// Database operation names for CREATE/DROP DATABASE
const (
	OpNameCreateDatabase = "CREATE_DATABASE"
	OpNameDropDatabase   = "DROP_DATABASE"
)

// DDL row key prefix for DDL operations in write intents
const DDLRowKeyPrefix = "__ddl__"

// DatabaseOperationSnapshot is a typed struct for CREATE/DROP DATABASE intents.
// Using a struct with msgpack tags ensures correct serialization/deserialization
// instead of relying on map[string]interface{} which can return []byte for strings.
type DatabaseOperationSnapshot struct {
	Type         int    `msgpack:"type"`
	Timestamp    int64  `msgpack:"timestamp"`
	DatabaseName string `msgpack:"database_name"`
	Operation    string `msgpack:"operation"`
}

// StatementTypeToOpCode converts protocol.StatementType to uint8 operation code
// This is the canonical implementation used across packages
// StatementType values: Insert=0, Replace=1, Update=2, Delete=3
func StatementTypeToOpCode(stmtType int) uint8 {
	switch stmtType {
	case 0, 1: // StatementInsert, StatementReplace
		return OpInsertInt
	case 2: // StatementUpdate
		return OpUpdateInt
	case 3: // StatementDelete
		return OpDeleteInt
	default:
		return OpInsertInt
	}
}
