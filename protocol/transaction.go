package protocol

import (
	"fmt"
	"sync"

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

// StatementType represents the type of SQL statement
type StatementType int

const (
	// DML - Data Manipulation
	StatementInsert StatementType = iota
	StatementReplace
	StatementUpdate
	StatementDelete
	StatementLoadData

	// DDL - Data Definition
	StatementDDL

	// DCL - Data Control Language (user/privilege management)
	StatementDCL

	// Transaction Control
	StatementBegin
	StatementCommit
	StatementRollback
	StatementSavepoint

	// XA Transaction
	StatementXA

	// Locking
	StatementLock

	// Query
	StatementSelect

	// Administrative
	StatementAdmin

	// Session variables (no-op)
	StatementSet

	// Database Management
	StatementShowDatabases
	StatementUseDatabase
	StatementCreateDatabase
	StatementDropDatabase

	// Metadata Queries (for DBeaver compatibility)
	StatementShowTables
	StatementShowColumns
	StatementShowCreateTable
	StatementShowIndexes
	StatementShowTableStatus
	StatementInformationSchema

	// Unsupported - invalid syntax or incompatible statement
	StatementUnsupported
)

// Statement represents a single SQL statement
type Statement struct {
	SQL       string
	Type      StatementType
	TableName string
	Database  string // Target database name
	Error     string // Error message if Type is StatementUnsupported
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
