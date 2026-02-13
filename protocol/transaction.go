package protocol

import (
	"fmt"
	"sync"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
)

// ConsistencyLevel defines read/write consistency requirements
type ConsistencyLevel int

const (
	ConsistencyLocalOne ConsistencyLevel = iota // Read/write from/to local node only
	ConsistencyOne                              // Read/write from/to one replica
	ConsistencyQuorum                           // Read/write from/to quorum (N/2 + 1)
	ConsistencyAll                              // Read/write from/to all replicas
)

// String returns string representation of consistency level
func (c ConsistencyLevel) String() string {
	switch c {
	case ConsistencyLocalOne:
		return "LOCAL_ONE"
	case ConsistencyOne:
		return "ONE"
	case ConsistencyQuorum:
		return "QUORUM"
	case ConsistencyAll:
		return "ALL"
	default:
		return "UNKNOWN"
	}
}

// ParseConsistencyLevel parses a string into ConsistencyLevel
func ParseConsistencyLevel(s string) (ConsistencyLevel, error) {
	switch s {
	case "LOCAL_ONE":
		return ConsistencyLocalOne, nil
	case "ONE":
		return ConsistencyOne, nil
	case "QUORUM":
		return ConsistencyQuorum, nil
	case "ALL":
		return ConsistencyAll, nil
	default:
		return ConsistencyLocalOne, fmt.Errorf("unknown consistency level: %s", s)
	}
}

// StatementCode is an alias for common.StatementCode to maintain backward compatibility
type StatementCode = common.StatementCode

// Statement code constants - aliased from common package for backward compatibility
const (
	StatementUnknown           = common.StatementUnknown
	StatementInsert            = common.StatementInsert
	StatementReplace           = common.StatementReplace
	StatementUpdate            = common.StatementUpdate
	StatementDelete            = common.StatementDelete
	StatementLoadData          = common.StatementLoadData
	StatementDDL               = common.StatementDDL
	StatementDCL               = common.StatementDCL
	StatementBegin             = common.StatementBegin
	StatementCommit            = common.StatementCommit
	StatementRollback          = common.StatementRollback
	StatementSavepoint         = common.StatementSavepoint
	StatementXA                = common.StatementXA
	StatementLock              = common.StatementLock
	StatementSelect            = common.StatementSelect
	StatementAdmin             = common.StatementAdmin
	StatementSet               = common.StatementSet
	StatementShowDatabases     = common.StatementShowDatabases
	StatementUseDatabase       = common.StatementUseDatabase
	StatementCreateDatabase    = common.StatementCreateDatabase
	StatementDropDatabase      = common.StatementDropDatabase
	StatementShowTables        = common.StatementShowTables
	StatementShowColumns       = common.StatementShowColumns
	StatementShowCreateTable   = common.StatementShowCreateTable
	StatementShowIndexes       = common.StatementShowIndexes
	StatementShowTableStatus   = common.StatementShowTableStatus
	StatementShowEngines       = common.StatementShowEngines
	StatementInformationSchema = common.StatementInformationSchema
	StatementUnsupported       = common.StatementUnsupported
	StatementSystemVariable    = common.StatementSystemVariable
	StatementVirtualTable      = common.StatementVirtualTable
)

// InformationSchemaTableType identifies which INFORMATION_SCHEMA table is being queried
type InformationSchemaTableType int

const (
	ISTableUnknown    InformationSchemaTableType = iota
	ISTableTables                                // INFORMATION_SCHEMA.TABLES
	ISTableColumns                               // INFORMATION_SCHEMA.COLUMNS
	ISTableSchemata                              // INFORMATION_SCHEMA.SCHEMATA
	ISTableStatistics                            // INFORMATION_SCHEMA.STATISTICS
)

// VirtualTableType identifies which Marmot virtual table is being queried
type VirtualTableType int

const (
	VirtualTableUnknown      VirtualTableType = iota
	VirtualTableClusterNodes                  // MARMOT_CLUSTER_NODES or MARMOT.CLUSTER_NODES
)

// InformationSchemaFilter holds extracted WHERE clause values for INFORMATION_SCHEMA queries
type InformationSchemaFilter struct {
	SchemaName string // From TABLE_SCHEMA = 'x' or SCHEMA_NAME = 'x'
	TableName  string // From TABLE_NAME = 'x'
	ColumnName string // From COLUMN_NAME = 'x'
}

// Statement represents a single SQL statement
type Statement struct {
	SQL       string        `msgpack:"SQL"`
	Type      StatementCode `msgpack:"Type"`
	TableName string        `msgpack:"TableName"`
	Database  string        `msgpack:"Database"`  // Target database name
	IntentKey []byte        `msgpack:"IntentKey"` // Intent key for MVCC conflict detection (binary format)
	Error     string        `msgpack:"Error"`     // Error message if Type is StatementUnsupported

	// CDC: Row-level change data (for DML operations)
	// Populated by preupdate hooks after local execution, sent to replicas instead of SQL
	OldValues map[string][]byte `msgpack:"OldValues"` // Before image (for UPDATE/DELETE)
	NewValues map[string][]byte `msgpack:"NewValues"` // After image (for INSERT/UPDATE/REPLACE)

	// ISFilter holds extracted WHERE clause values for INFORMATION_SCHEMA queries
	ISFilter InformationSchemaFilter

	// ISTableType identifies which INFORMATION_SCHEMA table (TABLES, COLUMNS, etc.)
	ISTableType InformationSchemaTableType

	// VirtualTableType identifies which Marmot virtual table (MARMOT_CLUSTER_NODES, etc.)
	VirtualTableType VirtualTableType

	// SystemVarNames lists system variables referenced (e.g., ["VERSION", "SQL_MODE", "DATABASE()"])
	SystemVarNames []string

	// ShowFilter holds the LIKE pattern for SHOW TABLES LIKE queries
	ShowFilter string

	// ExtractedParams holds literal values extracted during transpilation.
	// Used for local execution only - not serialized for CDC replication.
	// DML ships OldValues/NewValues via CDC, not SQL+params.
	ExtractedParams []interface{} `msgpack:"-"` // Exclude from msgpack serialization

	// LoadDataPayload carries LOAD DATA LOCAL INFILE file bytes for replicated
	// non-DML bulk-load transactions.
	LoadDataPayload []byte `msgpack:"LoadDataPayload,omitempty"`
}

// Transaction represents a buffered transaction
type Transaction struct {
	ID               uint64
	Statements       []Statement
	WriteConsistency ConsistencyLevel
	ReadConsistency  ConsistencyLevel
	Timestamp        hlc.Timestamp
	mu               sync.RWMutex
	inProgress       bool
}

// NewTransaction creates a new transaction buffer
func NewTransaction(id uint64) *Transaction {
	return &Transaction{
		ID:               id,
		Statements:       make([]Statement, 0),
		WriteConsistency: ConsistencyQuorum, // Default
		ReadConsistency:  ConsistencyLocalOne,
		inProgress:       true,
	}
}

// AddStatement adds a statement to the transaction buffer
func (t *Transaction) AddStatement(stmt Statement) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.inProgress {
		return fmt.Errorf("transaction %d is not in progress", t.ID)
	}

	t.Statements = append(t.Statements, stmt)
	return nil
}

// SetWriteConsistency sets the write consistency level
func (t *Transaction) SetWriteConsistency(level ConsistencyLevel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.WriteConsistency = level
}

// SetReadConsistency sets the read consistency level
func (t *Transaction) SetReadConsistency(level ConsistencyLevel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.ReadConsistency = level
}

// SetTimestamp sets the HLC timestamp for this transaction
func (t *Transaction) SetTimestamp(ts hlc.Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Timestamp = ts
}

// Commit marks the transaction as committed (ready for replication)
func (t *Transaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.inProgress {
		return fmt.Errorf("transaction %d is not in progress", t.ID)
	}

	t.inProgress = false
	return nil
}

// Rollback discards the transaction
func (t *Transaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.inProgress {
		return fmt.Errorf("transaction %d is not in progress", t.ID)
	}

	t.inProgress = false
	t.Statements = nil
	return nil
}

// IsInProgress returns true if the transaction is still in progress
func (t *Transaction) IsInProgress() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.inProgress
}

// GetStatements returns a copy of the statements
func (t *Transaction) GetStatements() []Statement {
	t.mu.RLock()
	defer t.mu.RUnlock()

	stmts := make([]Statement, len(t.Statements))
	copy(stmts, t.Statements)
	return stmts
}

// StatementCount returns the number of statements in the transaction
func (t *Transaction) StatementCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.Statements)
}

// HasWrites returns true if the transaction contains any write statements
func (t *Transaction) HasWrites() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	for _, stmt := range t.Statements {
		switch stmt.Type {
		case StatementInsert, StatementReplace, StatementUpdate, StatementDelete, StatementLoadData,
			StatementDDL, StatementDCL, StatementAdmin:
			return true
		}
	}
	return false
}
