package db

import "github.com/maxpert/marmot/protocol"

// IntentType distinguishes different kinds of write intents
type IntentType uint8

const (
	IntentTypeDML        IntentType = 0 // Regular row operations (INSERT/UPDATE/DELETE)
	IntentTypeDDL        IntentType = 1 // Schema operations (CREATE/ALTER/DROP TABLE)
	IntentTypeDatabaseOp IntentType = 2 // Database operations (CREATE/DROP DATABASE)
)

func (t IntentType) String() string {
	switch t {
	case IntentTypeDML:
		return "DML"
	case IntentTypeDDL:
		return "DDL"
	case IntentTypeDatabaseOp:
		return "DATABASE_OP"
	default:
		return "UNKNOWN"
	}
}

// OpType represents the type of data operation
type OpType uint8

const (
	OpTypeInsert  OpType = 0
	OpTypeReplace OpType = 1
	OpTypeUpdate  OpType = 2
	OpTypeDelete  OpType = 3
	OpTypeDelta   OpType = 4 // Used for LWW delta sync operations
)

func (o OpType) String() string {
	switch o {
	case OpTypeInsert:
		return "INSERT"
	case OpTypeReplace:
		return "REPLACE"
	case OpTypeUpdate:
		return "UPDATE"
	case OpTypeDelete:
		return "DELETE"
	case OpTypeDelta:
		return "DELTA"
	default:
		return "UNKNOWN"
	}
}

// TxnStatus represents transaction state
type TxnStatus uint8

const (
	TxnStatusPending   TxnStatus = 0
	TxnStatusCommitted TxnStatus = 1
	TxnStatusAborted   TxnStatus = 2
)

func (s TxnStatus) String() string {
	switch s {
	case TxnStatusPending:
		return "PENDING"
	case TxnStatusCommitted:
		return "COMMITTED"
	case TxnStatusAborted:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// SyncStatus represents replication sync state
type SyncStatus uint8

const (
	SyncStatusSynced     SyncStatus = 0
	SyncStatusCatchingUp SyncStatus = 1
	SyncStatusFailed     SyncStatus = 2
)

func (s SyncStatus) String() string {
	switch s {
	case SyncStatusSynced:
		return "SYNCED"
	case SyncStatusCatchingUp:
		return "CATCHING_UP"
	case SyncStatusFailed:
		return "FAILED"
	default:
		return "UNKNOWN"
	}
}

// DatabaseOpType represents CREATE/DROP DATABASE operations
type DatabaseOpType uint8

const (
	DatabaseOpCreate DatabaseOpType = 0
	DatabaseOpDrop   DatabaseOpType = 1
)

func (o DatabaseOpType) String() string {
	switch o {
	case DatabaseOpCreate:
		return "CREATE_DATABASE"
	case DatabaseOpDrop:
		return "DROP_DATABASE"
	default:
		return "UNKNOWN"
	}
}

// DatabaseOperationSnapshot is a typed struct for CREATE/DROP DATABASE intents
type DatabaseOperationSnapshot struct {
	Type         int            `msgpack:"type"`
	Timestamp    int64          `msgpack:"timestamp"`
	DatabaseName string         `msgpack:"database_name"`
	Operation    DatabaseOpType `msgpack:"operation"`
}

// DDLSnapshot is a typed struct for DDL operation intents
type DDLSnapshot struct {
	Type      int    `msgpack:"type"`
	Timestamp int64  `msgpack:"timestamp"`
	SQL       string `msgpack:"sql"`
	TableName string `msgpack:"table_name"`
}

// StatementTypeToOpType converts protocol.StatementCode to OpType.
// Uses typed enum constants for compile-time safety.
func StatementTypeToOpType(stmtType protocol.StatementCode) OpType {
	switch stmtType {
	case protocol.StatementInsert:
		return OpTypeInsert
	case protocol.StatementReplace:
		return OpTypeReplace
	case protocol.StatementUpdate:
		return OpTypeUpdate
	case protocol.StatementDelete:
		return OpTypeDelete
	default:
		return OpTypeInsert
	}
}
