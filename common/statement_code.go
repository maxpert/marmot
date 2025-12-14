// Package common provides shared types used across the codebase.
// HARD RULE: StatementCode is defined HERE and ONLY HERE.
// Both protocol and query packages use this type directly.
package common

// StatementCode categorizes SQL statements for execution routing and validation.
// This is the INTERNAL representation - use grpc/common.StatementType for wire format.
type StatementCode int

const (
	StatementUnknown StatementCode = iota // 0 - means not yet classified
	StatementInsert
	StatementReplace
	StatementUpdate
	StatementDelete
	StatementLoadData
	StatementDDL
	StatementDCL
	StatementBegin
	StatementCommit
	StatementRollback
	StatementSavepoint
	StatementXA
	StatementLock
	StatementSelect
	StatementAdmin
	StatementSet
	StatementShowDatabases
	StatementUseDatabase
	StatementCreateDatabase
	StatementDropDatabase
	StatementShowTables
	StatementShowColumns
	StatementShowCreateTable
	StatementShowIndexes
	StatementShowTableStatus
	StatementInformationSchema
	StatementUnsupported
	StatementSystemVariable // SELECT @@version, SELECT DATABASE(), etc.
	StatementVirtualTable   // SELECT * FROM MARMOT_CLUSTER_NODES, etc.
)

// IsMutation returns true if the statement type is a mutation operation.
func (t StatementCode) IsMutation() bool {
	switch t {
	case StatementInsert, StatementReplace, StatementUpdate, StatementDelete,
		StatementLoadData, StatementDDL, StatementDCL, StatementAdmin,
		StatementCreateDatabase, StatementDropDatabase:
		return true
	}
	return false
}

// IsReadOnly returns true if the statement type is read-only.
func (t StatementCode) IsReadOnly() bool {
	switch t {
	case StatementSelect, StatementShowDatabases, StatementShowTables,
		StatementShowColumns, StatementShowCreateTable, StatementShowIndexes,
		StatementShowTableStatus, StatementInformationSchema:
		return true
	}
	return false
}
