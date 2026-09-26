package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

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
// AUTO_INCREMENT column implies right now: max(the marker's own declared
// floor, the largest existing id the column's width can hold). ok is false
// when the table carries no explicitly declared AUTO_INCREMENT marker.
//
// Ids above the width are left out. The allocator can never issue one, so no
// base has to clear them, and counting them would put the base past the
// ceiling and make every later insert fail as exhausted - the table the width
// check exempts (checkAutoIncWidthCeilings) would be unusable instead.
func autoIncSeedFloor(q rowQuerier, table string) (floor uint64, ok bool, err error) {
	col, attrs, marked, err := autoIncMarkedColumn(q, table)
	if err != nil || !marked {
		return 0, false, err
	}

	var max sql.NullInt64
	query := fmt.Sprintf("SELECT MAX(%s) FROM %s WHERE %s <= ?", quoteIdent(col), quoteIdent(table), quoteIdent(col))
	if scanErr := q.QueryRow(query, int64(attrs.WidthMax())).Scan(&max); scanErr != nil {
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

// schemaQuerier is the read surface tableDefinitions needs: *sql.DB,
// *sql.Conn and *sql.Tx all satisfy it.
type schemaQuerier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

// tableDefinitions returns every table in q's schema, by name, with the
// CREATE statement sqlite_master holds for it.
func tableDefinitions(ctx context.Context, q schemaQuerier) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, "SELECT name, sql FROM sqlite_master WHERE type = 'table'")
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	defer rows.Close()

	defs := make(map[string]string)
	for rows.Next() {
		var name string
		var def sql.NullString
		if err := rows.Scan(&name, &def); err != nil {
			return nil, fmt.Errorf("list tables: %w", err)
		}
		defs[name] = def.String
	}
	return defs, rows.Err()
}

// endedIncarnations returns, sorted, every table name whose incarnation one DDL
// statement ended: a table it removed, created or redefined. A redefinition
// includes re-declaring the AUTO_INCREMENT column's width; any other change
// to the table's definition ends the incarnation too, which costs the
// unissued part of a range and nothing else.
func endedIncarnations(before, after map[string]string) []string {
	var changed []string
	for name, def := range before {
		if newDef, ok := after[name]; !ok || newDef != def {
			changed = append(changed, name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			changed = append(changed, name)
		}
	}
	sort.Strings(changed)
	return changed
}

// autoIncInheritance names a table that one DDL statement brought into being
// while it removed others: a RENAME, or any statement with the same effect.
// The table may hold the removed tables' rows, and with them ids granted
// under their names, so its claim base must reach theirs (AutoIncClaimStore.Inherit).
type autoIncInheritance struct {
	table string
	from  []string
	owner uint64
}

// inheritedTables compares a database's tables before and after one DDL
// statement. When the statement removed tables, every table it created is
// treated as having inherited their rows. It is conservative on purpose: a
// statement that dropped one table and created an unrelated one only raises
// the new table's base, which burns ids but can never reissue one, while a
// missed rename could.
func inheritedTables(before, after map[string]string, owner uint64) []autoIncInheritance {
	var removed []string
	for name := range before {
		if _, ok := after[name]; !ok {
			removed = append(removed, name)
		}
	}
	if len(removed) == 0 {
		return nil
	}
	sort.Strings(removed)
	var inherited []autoIncInheritance
	for name := range after {
		if _, ok := before[name]; !ok {
			inherited = append(inherited, autoIncInheritance{table: name, from: removed, owner: owner})
		}
	}
	return inherited
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

// seedAutoIncBasesForDDL first raises every table in inherited to the bases of
// the tables it took the place of (AutoIncClaimStore.Inherit), then seeds
// __marmot__autoinc, in the system database, for every table named by a DDL
// intent in this transaction that carries an explicitly declared
// AUTO_INCREMENT marker. Both only ever raise a base. Seeding is a no-op for a
// table with no such marker.
//
// The seed floor is derived from THIS node's own sqlite_master and MAX(id) on
// the USER database, read through q after the DDL has executed - not by parsing
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
func (tm *TransactionManager) seedAutoIncBasesForDDL(q rowQuerier, inherited []autoIncInheritance, tables []ddlTableOwner) error {
	claimStore, dbName := tm.autoIncClaimStoreAndDatabaseName()
	for _, in := range inherited {
		if claimStore == nil {
			return fmt.Errorf("inherit auto-increment base for %s.%s: no auto-increment claim store wired", dbName, in.table)
		}
		if err := claimStore.Inherit(dbName, in.table, in.from, in.owner); err != nil {
			return err
		}
	}

	seen := make(map[string]bool, len(tables))
	for _, t := range tables {
		if t.table == "" || seen[t.table] {
			continue
		}
		seen[t.table] = true

		floor, ok, err := autoIncSeedFloor(q, t.table)
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
