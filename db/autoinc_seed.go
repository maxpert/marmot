package db

import (
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
)

// rowQuerier is the read surface autoIncMarkedColumn needs. Both *sql.DB (the
// seed-at-commit path below, which runs after the DDL has already committed
// to this node's schema) and *sql.Tx (the widthMax check at PREPARE, in
// db/ddl_prepare_validate.go, which must see the validation transaction's own
// in-flight schema before it rolls back) satisfy it.
type rowQuerier interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

// autoIncMarkedColumn derives, from a table's OWN sqlite_master row read
// through q, the column carrying an explicitly declared AUTO_INCREMENT
// marker - the same derivation AutoIncWidthMax trusts, generalised to read
// from raw DDL text instead of the schema cache so it also works inside a
// not-yet-committed validation transaction.
//
// ok is false, with no error, when the table does not exist or carries no
// explicitly declared AUTO_INCREMENT marker: neither is a failure, both mean
// "this rule does not apply to this table."
func autoIncMarkedColumn(q rowQuerier, table string) (col string, attrs intmarker.Attributes, ok bool, err error) {
	var createSQL string
	scanErr := q.QueryRow("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?", table).Scan(&createSQL)
	switch {
	case scanErr == sql.ErrNoRows:
		return "", intmarker.Attributes{}, false, nil
	case scanErr != nil:
		return "", intmarker.Attributes{}, false, fmt.Errorf("read sqlite_master for %s: %w", table, scanErr)
	}

	for name, a := range intmarker.Decode(createSQL) {
		if a.Marked() && a.ExplicitAutoInc {
			return name, a, true, nil
		}
	}
	return "", intmarker.Attributes{}, false, nil
}

// autoIncSeedFloor computes the base floor a table's DDL-declared
// AUTO_INCREMENT column implies right now: max(MAX(<col>) over the table's
// existing rows, the marker's own declared floor). ok is false when the table
// carries no explicitly declared AUTO_INCREMENT marker.
func autoIncSeedFloor(q rowQuerier, table string) (floor uint64, ok bool, err error) {
	col, attrs, marked, err := autoIncMarkedColumn(q, table)
	if err != nil || !marked {
		return 0, false, err
	}

	var max sql.NullInt64
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(col), quoteIdent(table))
	if scanErr := q.QueryRow(query).Scan(&max); scanErr != nil {
		return 0, false, fmt.Errorf("compute existing max for %s.%s: %w", table, col, scanErr)
	}

	// A MAX at or below zero raises nothing: ids are issued upward from a
	// non-negative base, so non-positive rows can never collide with one, and
	// MySQL likewise starts such a table's counter at 1.
	floor = attrs.AutoIncFloor
	if max.Valid && max.Int64 > 0 && uint64(max.Int64) > floor {
		floor = uint64(max.Int64)
	}
	return floor, true, nil
}

// ddlTableOwner names one table a DDL intent in the current transaction
// touched, paired with the node that authored that intent. The owner comes
// from the intent record's own NodeID - identical on every node applying the
// same replicated intent - never from local state, exactly as
// AutoIncClaimStore.ApplyClaims records intent.NodeID as a claim's owner rather than
// trusting anything computed locally.
type ddlTableOwner struct {
	table string
	owner uint64
}

// seedAutoIncBasesForDDL seeds __marmot__autoinc, in the system database, for
// every table named by a DDL intent in this transaction that carries an
// explicitly declared AUTO_INCREMENT marker. It is a
// no-op for a table with no such marker.
//
// The seed floor is derived from THIS node's own sqlite_master and MAX(id) on
// the USER database (tm.db), read after the DDL has executed - not by parsing
// the DDL statement text - so the same logic uniformly covers CREATE TABLE and
// every ALTER shape that can leave a marker in sqlite_master. That derivation
// is unchanged by the claim table's relocation to the system database; only
// where the floor is written has moved.
//
// The write itself goes through tm.autoIncClaimStore, injected by
// DatabaseManager.wireGCCoordination (db/database_manager.go) and always
// backed by the system database (db/autoinc_claim.go). A marked table found
// with no store wired is a configuration error, not a "nothing to do" case,
// so it fails fast rather than silently skipping the seed.
func (tm *TransactionManager) seedAutoIncBasesForDDL(tables []ddlTableOwner) error {
	seen := make(map[string]bool, len(tables))
	claimStore, dbName := tm.autoIncClaimStoreAndDatabaseName()
	for _, t := range tables {
		if t.table == "" || seen[t.table] {
			continue
		}
		seen[t.table] = true

		floor, ok, err := autoIncSeedFloor(tm.db, t.table)
		if err != nil {
			return fmt.Errorf("derive auto-increment seed for %s: %w", t.table, err)
		}
		if !ok {
			continue
		}
		if claimStore == nil {
			return fmt.Errorf("seed auto-increment base for %s.%s: no auto-increment claim store wired", dbName, t.table)
		}
		if err := claimStore.Seed(dbName, t.table, floor, t.owner); err != nil {
			return err
		}
	}
	return nil
}
