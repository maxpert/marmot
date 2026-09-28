package db

import (
	"fmt"

	"github.com/maxpert/marmot/protocol"
)

// IntentType distinguishes different kinds of write intents
type IntentType uint8

const (
	IntentTypeDML        IntentType = 0 // Regular row operations (INSERT/UPDATE/DELETE)
	IntentTypeDDL        IntentType = 1 // Schema operations (CREATE/ALTER/DROP TABLE)
	IntentTypeDatabaseOp IntentType = 2 // Database operations (CREATE/DROP DATABASE)
	// IntentTypeAutoIDClaim is a narrow auto-increment range claim.
	//
	// It must NOT be IntentTypeDML, and that is a correctness requirement
	// rather than a taxonomy preference. WriteIntent short-circuits
	// IntentTypeDML into storeDMLIntent, whose record omits DataSnapshot and
	// which writes only to an in-memory map; GetIntentsByTxn iterates the
	// Pebble prefix only, so a claim stored that way would never be returned
	// and its payload would never have existed. The COMMIT handler reads
	// exactly that payload to write the new base, so a claim on the DML branch
	// would pass PREPARE, apply nothing at COMMIT, leave every participant's
	// base where it was, and let the next claimant anywhere compute the same
	// range and mint the same ids. Selecting storage is precisely what this
	// type is for.
	IntentTypeAutoIDClaim IntentType = 3
)

func (t IntentType) String() string {
	switch t {
	case IntentTypeDML:
		return "DML"
	case IntentTypeDDL:
		return "DDL"
	case IntentTypeDatabaseOp:
		return "DATABASE_OP"
	case IntentTypeAutoIDClaim:
		return "AUTO_ID_CLAIM"
	default:
		return "UNKNOWN"
	}
}

// OpType represents the type of data operation
type OpType uint8

const (
	OpTypeInsert      OpType = 0
	OpTypeReplace     OpType = 1
	OpTypeUpdate      OpType = 2
	OpTypeDelete      OpType = 3
	OpTypeDelta       OpType = 4 // Used for LWW delta sync operations
	OpTypeDDL         OpType = 5 // DDL schema changes (CREATE/DROP/ALTER TABLE)
	OpTypeLoadData    OpType = 6 // LOAD DATA LOCAL INFILE bulk load
	OpTypeVectorIndex OpType = 7 // Vector index control metadata
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
	case OpTypeDDL:
		return "DDL"
	case OpTypeLoadData:
		return "LOAD_DATA"
	case OpTypeVectorIndex:
		return "VECTOR_INDEX"
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
	// Generation is the registry generation PREPARE resolved for this op (the
	// coordinator's stamp, or - when unstamped - this participant's own
	// locally computed value; see ReplicationEngine.prepareDatabaseOperation).
	// COMMIT combines it with Operation to form the DatabaseRegistryKey it
	// applies through DatabaseManager.ApplyDatabaseOp.
	Generation uint64 `msgpack:"generation"`
}

// DDLSnapshot is a typed struct for DDL operation intents
type DDLSnapshot struct {
	Type      int    `msgpack:"type"`
	Timestamp int64  `msgpack:"timestamp"`
	SQL       string `msgpack:"sql"`
	TableName string `msgpack:"table_name"`
}

// LoadDataSnapshot is a typed struct for LOAD DATA LOCAL INFILE intents.
type LoadDataSnapshot struct {
	Type      int    `msgpack:"type"`
	Timestamp int64  `msgpack:"timestamp"`
	SQL       string `msgpack:"sql"`
	TableName string `msgpack:"table_name"`
	Data      []byte `msgpack:"data"`
}

// StatementTypeToOpType converts protocol.StatementCode to OpType.
// Panics on unknown/unsupported statement type - only DML statements have CDC op types.
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
		panic(fmt.Sprintf("StatementTypeToOpType: unsupported statement type %d (only DML types are supported)", stmtType))
	}
}

// OpTypeToStatementType converts OpType to protocol.StatementCode.
// Panics on unknown/unsupported op type - only DML operations have statement types.
func OpTypeToStatementType(op OpType) protocol.StatementCode {
	switch op {
	case OpTypeInsert:
		return protocol.StatementInsert
	case OpTypeReplace:
		return protocol.StatementReplace
	case OpTypeUpdate:
		return protocol.StatementUpdate
	case OpTypeDelete:
		return protocol.StatementDelete
	case OpTypeDDL:
		return protocol.StatementDDL
	case OpTypeLoadData:
		return protocol.StatementLoadData
	case OpTypeVectorIndex:
		return protocol.StatementVectorIndexControl
	default:
		panic(fmt.Sprintf("OpTypeToStatementType: unsupported op type %d", op))
	}
}
