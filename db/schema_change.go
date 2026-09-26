package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
)

// sqlExecQuerier is what running one DDL statement between two reads of the
// schema needs. *sql.Conn and *sql.Tx both satisfy it.
type sqlExecQuerier interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// SchemaChange is what a sequence of DDL statements did to one database's
// tables, as AUTO_INCREMENT claims need to know it: which table incarnations
// ended, which tables took the place of removed ones, and which tables to
// seed a claim base for.
//
// Every path that changes a database's schema on a node records its change
// here and finishes it the same way, in this order:
//  1. TransactionManager.endIncarnations, before the new schema is visible to
//     queries, so no insert into a new incarnation takes an id from a range
//     claimed for an old one;
//  2. TransactionManager.seedSchemaChange, which raises inherited bases and
//     seeds every touched table from its own MAX(id).
//
// The paths are the 2PC COMMIT (applyNonDMLIntents), anti-entropy replay
// (ReplicatedDatabase.ApplyReplayedDDL) and a restored database's reattach
// (DatabaseManager.AttachDatabase).
type SchemaChange struct {
	ended     []string
	inherited []autoIncInheritance
	tables    []ddlTableOwner
}

// Exec runs one DDL statement on q, with the database's table definitions
// read on q just before and just after it, and records what it changed.
// table is the table the statement names, owner the node that authored it.
func (c *SchemaChange) Exec(ctx context.Context, q sqlExecQuerier, stmt, table string, owner uint64) error {
	before, err := tableDefinitions(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to read tables before DDL: %w", err)
	}
	if _, err := q.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("failed to execute DDL statement: %w", err)
	}
	after, err := tableDefinitions(ctx, q)
	if err != nil {
		return fmt.Errorf("failed to read tables after DDL: %w", err)
	}
	c.record(before, after, owner)
	c.tables = append(c.tables, ddlTableOwner{table: table, owner: owner})
	return nil
}

// record adds one before/after pair of table definitions to the change.
func (c *SchemaChange) record(before, after map[string]string, owner uint64) {
	c.ended = append(c.ended, endedIncarnations(before, after)...)
	for _, in := range inheritedTables(before, after, owner) {
		c.inherited = append(c.inherited, in)
		c.tables = append(c.tables, ddlTableOwner{table: in.table, owner: owner})
	}
}

// Empty reports whether the change recorded nothing.
func (c *SchemaChange) Empty() bool {
	return len(c.ended) == 0 && len(c.inherited) == 0 && len(c.tables) == 0
}

// endIncarnations reports every table incarnation the change ended to the
// claim store's listener.
func (tm *TransactionManager) endIncarnations(change *SchemaChange) {
	for _, name := range change.ended {
		tm.autoIncIncarnationEnded(name)
	}
}

// seedSchemaChange raises the claim bases the change requires, reading the
// seed floors through q: the connection or transaction that sees the
// change's schema and rows.
func (tm *TransactionManager) seedSchemaChange(q rowQuerier, change *SchemaChange) error {
	if len(change.tables) == 0 {
		return nil
	}
	return tm.seedAutoIncBasesForDDL(q, change.inherited, change.tables)
}

// ApplyReplayedDDL runs one replayed DDL statement, rewritten for idempotent
// replay, in the replay's transaction and records it in change. The caller
// finishes change with FinishReplayedSchemaChange in the same transaction
// before committing it.
func (mdb *ReplicatedDatabase) ApplyReplayedDDL(ctx context.Context, tx *sql.Tx, ddlSQL, table string, owner uint64, change *SchemaChange) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	return change.Exec(ctx, tx, protocol.RewriteDDLForIdempotency(ddlSQL), table, owner)
}

// FinishReplayedSchemaChange ends the incarnations change ended and raises
// the claim bases it requires, reading floors inside tx: tx holds this
// database's only write connection, so reading through anything else would
// wait on it. It runs before tx commits, so the new schema is not yet
// visible when the incarnations end, and a failure rolls the replay back to
// be retried whole.
func (mdb *ReplicatedDatabase) FinishReplayedSchemaChange(tx *sql.Tx, change *SchemaChange) error {
	if change.Empty() {
		return nil
	}
	mdb.txnMgr.endIncarnations(change)
	return mdb.txnMgr.seedSchemaChange(tx, change)
}
