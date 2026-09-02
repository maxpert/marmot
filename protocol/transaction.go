package protocol

import (
	"fmt"
	"sync"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"vitess.io/vitess/go/vt/sqlparser"
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
	StatementUnknown            = common.StatementUnknown
	StatementInsert             = common.StatementInsert
	StatementReplace            = common.StatementReplace
	StatementUpdate             = common.StatementUpdate
	StatementDelete             = common.StatementDelete
	StatementLoadData           = common.StatementLoadData
	StatementDDL                = common.StatementDDL
	StatementDCL                = common.StatementDCL
	StatementBegin              = common.StatementBegin
	StatementCommit             = common.StatementCommit
	StatementRollback           = common.StatementRollback
	StatementSavepoint          = common.StatementSavepoint
	StatementXA                 = common.StatementXA
	StatementLock               = common.StatementLock
	StatementSelect             = common.StatementSelect
	StatementAdmin              = common.StatementAdmin
	StatementSet                = common.StatementSet
	StatementShowDatabases      = common.StatementShowDatabases
	StatementUseDatabase        = common.StatementUseDatabase
	StatementCreateDatabase     = common.StatementCreateDatabase
	StatementDropDatabase       = common.StatementDropDatabase
	StatementShowTables         = common.StatementShowTables
	StatementShowColumns        = common.StatementShowColumns
	StatementShowCreateTable    = common.StatementShowCreateTable
	StatementShowIndexes        = common.StatementShowIndexes
	StatementShowTableStatus    = common.StatementShowTableStatus
	StatementShowEngines        = common.StatementShowEngines
	StatementInformationSchema  = common.StatementInformationSchema
	StatementUnsupported        = common.StatementUnsupported
	StatementSystemVariable     = common.StatementSystemVariable
	StatementVirtualTable       = common.StatementVirtualTable
	StatementCreateVectorIndex  = common.StatementCreateVectorIndex
	StatementDropVectorIndex    = common.StatementDropVectorIndex
	StatementReindexVectorIndex = common.StatementReindexVectorIndex
	StatementVectorIndexControl = common.StatementVectorIndexControl
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
	// Decoded local apply state. Replication sends EncodedRow, not raw SQL.
	OldValues map[string][]byte `msgpack:"OldValues"` // Before image (for UPDATE/DELETE)
	NewValues map[string][]byte `msgpack:"NewValues"` // After image (for INSERT/UPDATE/REPLACE)
	Operation uint8             `msgpack:"Operation,omitempty"`

	// EncodedRow carries the canonical msgpack EncodedCapturedRow bytes.
	EncodedRow   []byte `msgpack:"EncodedRow,omitempty"`
	EncodedCodec uint32 `msgpack:"EncodedCodec,omitempty"`

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

	// Vector index metadata (populated for CREATE/DROP VECTOR INDEX)
	VectorIndexName   string                    `msgpack:"-"` // Index name
	VectorColumnName  string                    `msgpack:"-"` // Column being indexed
	VectorMetric      string                    `msgpack:"-"` // cosine, l2, dot
	VectorDim         int                       `msgpack:"-"` // Vector dimensionality
	VectorNlist       int                       `msgpack:"-"` // Number of IVF centroids (0 = auto-tune)
	VectorNprobe      int                       `msgpack:"-"` // Clusters searched per query (0 = auto-tune)
	VectorMaxNorm     float32                   `msgpack:"-"` // Max norm for dot-product augmentation
	VectorIndexChange *common.VectorIndexChange `msgpack:"VectorIndexChange,omitempty"`

	// ExtractedParams holds literal values extracted during transpilation.
	// Used for local execution only - not serialized for CDC replication.
	// DML ships OldValues/NewValues via CDC, not SQL+params.
	ExtractedParams []interface{} `msgpack:"-"` // Exclude from msgpack serialization

	// ParamOrder is set only when SQL mixes caller-supplied bind placeholders
	// with ExtractedParams (e.g. an auto-increment id injected alongside a
	// prepared statement's own `?` marks): for every placeholder in SQL, in
	// left-to-right order, true means "take the next wire-supplied value" and
	// false means "take the next value from ExtractedParams". nil means only
	// one of the two sources is in play - see MergeExecParams.
	ParamOrder []bool `msgpack:"-"`

	// ParsedAST carries the Vitess AST produced during MySQL-dialect parse.
	// Non-nil only when the pipeline parsed the statement via Vitess (SELECT,
	// DML, and most DDL). Downstream components that need the AST — notably
	// coordinator/vec_handler's vec_match rewriter — use this to avoid a
	// second parse. Never serialized over the wire: the AST holds non-portable
	// Go pointers and replicated peers re-parse from SQL.
	//
	// Consumers must treat the value as read-only; pass sqlparser.Clone(ast)
	// into any builder that mutates node fields.
	ParsedAST sqlparser.Statement `msgpack:"-"`

	// LoadDataPayload carries LOAD DATA LOCAL INFILE file bytes for replicated
	// non-DML bulk-load transactions.
	LoadDataPayload []byte `msgpack:"LoadDataPayload,omitempty"`
}

// MergeExecParams produces the final positional argument list for executing
// s.SQL against SQLite, combining the caller's wire-supplied params with
// s.ExtractedParams (literals the pipeline pulled out of the SQL text, e.g. a
// server-injected auto-increment id).
//
// When s.ParamOrder is nil, exactly one of the two sources is in play - the
// common case - so whichever is non-empty is used as-is. When s.ParamOrder is
// set, SQL contains a mix of the caller's own `?` placeholders and
// pipeline-extracted ones interleaved in serialization order; ParamOrder
// records that order (true = next wireParams value, false = next
// ExtractedParams value) so the two sources are threaded back together
// positionally instead of one being silently dropped.
func (s Statement) MergeExecParams(wireParams []interface{}) []interface{} {
	if len(s.ParamOrder) == 0 {
		if len(wireParams) == 0 && len(s.ExtractedParams) > 0 {
			return s.ExtractedParams
		}
		return wireParams
	}

	merged := make([]interface{}, 0, len(s.ParamOrder))
	wireIdx, extractedIdx := 0, 0
	for _, fromWire := range s.ParamOrder {
		if fromWire {
			if wireIdx < len(wireParams) {
				merged = append(merged, wireParams[wireIdx])
			}
			wireIdx++
			continue
		}
		if extractedIdx < len(s.ExtractedParams) {
			merged = append(merged, s.ExtractedParams[extractedIdx])
		}
		extractedIdx++
	}
	return merged
}

// WithResolvedParams returns a copy of s for executing different SQL whose
// params are already fully resolved and positional - e.g. a vector-search
// rewrite's primary or fallback query, where params has already been
// computed from the original statement's bound values. It clears
// ParamOrder: the copy's params are complete on their own, and s.ParamOrder
// (if any) describes sql's placeholder layout, not the new one, so
// MergeExecParams must not try to interleave them against a second source.
func (s Statement) WithResolvedParams(sql string, params []interface{}) Statement {
	s.SQL = sql
	s.ExtractedParams = params
	s.ParamOrder = nil
	return s
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
