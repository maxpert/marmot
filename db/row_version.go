package db

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol/filter"
)

// rowVersionTableDDL creates the per-row version table. Every replicated row
// change stamps its row's key with the change's commit version (the
// transaction's commit HLC, tie-broken by its origin node), in the same
// SQLite transaction as the change itself; a change older than the stored
// version is skipped. deleted marks a tombstone: the version of a DELETE,
// kept so an older INSERT or UPDATE cannot bring the row back. pk is the
// row's intent key exactly as the CDC hook captured it.
const rowVersionTableDDL = `
CREATE TABLE IF NOT EXISTS __marmot_row_version (
	tbl TEXT NOT NULL,
	pk BLOB NOT NULL,
	wall INTEGER NOT NULL,
	logical INTEGER NOT NULL,
	node INTEGER NOT NULL,
	deleted INTEGER NOT NULL,
	PRIMARY KEY (tbl, pk)
) WITHOUT ROWID`

// rowVersionTombstoneIndexDDL indexes tombstones by age, so the purge finds
// them without scanning every live row's version.
const rowVersionTombstoneIndexDDL = `
CREATE INDEX IF NOT EXISTS __marmot_row_version_tombstones ON __marmot_row_version (wall) WHERE deleted = 1`

// stampRowVersionSQL stores a version unless the stored one is newer. Equal
// versions belong to the same transaction, which may change a row twice, so
// they win too. Zero rows changed means the stored version is newer.
const stampRowVersionSQL = stampRowVersionInsert + stampRowVersionTuple + stampRowVersionUpsert

const (
	stampRowVersionInsert = "INSERT INTO __marmot_row_version (tbl, pk, wall, logical, node, deleted) VALUES "
	stampRowVersionTuple  = "(?, ?, ?, ?, ?, ?)"
	stampRowVersionUpsert = `
ON CONFLICT (tbl, pk) DO UPDATE SET
	wall = excluded.wall, logical = excluded.logical, node = excluded.node, deleted = excluded.deleted
WHERE (excluded.wall, excluded.logical, excluded.node) >= (wall, logical, node)`
)

// stampRowVersionsSQL is stampRowVersionSQL for n rows at once.
func stampRowVersionsSQL(n int) string {
	values := strings.TrimSuffix(strings.Repeat(stampRowVersionTuple+", ", n), ", ")
	return stampRowVersionInsert + values + stampRowVersionUpsert
}

// ensureRowVersionTable creates __marmot_row_version and its tombstone
// index in db if they are absent.
func ensureRowVersionTable(db *sql.DB) error {
	if db == nil {
		return nil
	}
	if _, err := db.Exec(rowVersionTableDDL); err != nil {
		return fmt.Errorf("create row version table: %w", err)
	}
	if _, err := db.Exec(rowVersionTombstoneIndexDDL); err != nil {
		return fmt.Errorf("create row version tombstone index: %w", err)
	}
	return nil
}

// versionedApplier applies replicated row changes inside one SQLite
// transaction, each versioned per row, with the version stamp prepared once
// for the transaction.
type versionedApplier struct {
	tx     *sql.Tx
	schema CDCSchemaProvider
	stamp  *sql.Stmt
}

// newVersionedApplier prepares the version stamp on tx. The caller closes
// the applier before tx ends.
func newVersionedApplier(tx *sql.Tx, schema CDCSchemaProvider) (*versionedApplier, error) {
	stamp, err := tx.Prepare(stampRowVersionSQL)
	if err != nil {
		return nil, fmt.Errorf("prepare row version stamp: %w", err)
	}
	return &versionedApplier{tx: tx, schema: schema, stamp: stamp}, nil
}

// Close releases the prepared stamp.
func (a *versionedApplier) Close() error {
	return a.stamp.Close()
}

// stampVersion stores version for (table, key) unless a newer version is
// already stored, and reports whether it was stored: whether the change
// carrying it may be applied.
func (a *versionedApplier) stampVersion(table string, key []byte, version hlc.Timestamp, deleted bool) (bool, error) {
	res, err := a.stamp.Exec(table, key, version.WallTime, version.Logical, version.NodeID, deleted)
	if err != nil {
		return false, fmt.Errorf("stamp row version for %s: %w", table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("stamp row version for %s: %w", table, err)
	}
	return n > 0, nil
}

// pkKeyFromValues encodes the key of the row whose column values are values
// exactly as the preupdate hook encodes it at capture (extractPKFromValues):
// the hook sees TEXT and BLOB alike as []byte, so both map to PKTypeBytes,
// and every integer width decoded from msgpack maps to PKTypeInt64. rowid
// tables carry their rowid under rowidColumnKey, which is also their only
// primary key name. A key column values lacks is read from fallback, as
// ApplyCDCUpdate's WHERE clause does.
func pkKeyFromValues(table string, primaryKeys []string, values, fallback map[string][]byte) ([]byte, error) {
	pkValues := make([]filter.TypedPKValue, len(primaryKeys))
	for i, col := range primaryKeys {
		encoded, ok := values[col]
		if !ok {
			encoded, ok = fallback[col]
		}
		if !ok {
			return nil, fmt.Errorf("row version %s: primary key column %s missing", table, col)
		}
		v, err := unmarshalCDCValue(encoded)
		if err != nil {
			return nil, fmt.Errorf("row version %s: decode primary key column %s: %w", table, col, err)
		}
		pkValues[i] = capturedPKValue(v)
	}
	return filter.EncodeIntentKey(table, pkValues), nil
}

// capturedPKValue maps a decoded CDC value to the typed PK value the hook
// captured for it.
func capturedPKValue(v interface{}) filter.TypedPKValue {
	rv := reflect.ValueOf(v)
	switch {
	case v == nil:
		return filter.TypedPKValue{Type: filter.PKTypeNull}
	case rv.CanInt():
		return valueToTypedPK(rv.Int())
	case rv.CanUint():
		return valueToTypedPK(int64(rv.Uint()))
	case rv.CanFloat():
		return valueToTypedPK(rv.Float())
	}
	if s, ok := v.(string); ok {
		return valueToTypedPK([]byte(s))
	}
	return valueToTypedPK(v)
}

// apply applies one replicated DML row change, versioned
// by the committing transaction's commit timestamp: it stamps the row's
// version first and applies the change only if no newer version is stored,
// reporting whether it did. A DELETE leaves a tombstone. An UPDATE whose
// row is absent inserts its after-image; one that moves the row to a new
// primary key also tombstones the old key, and leaves the old row alone if
// a newer change owns it.
func (a *versionedApplier) apply(opType OpType, table string, intentKey []byte, oldValues, newValues map[string][]byte, version hlc.Timestamp) (bool, error) {
	if len(intentKey) == 0 {
		return false, fmt.Errorf("row version %s: change carries no intent key", table)
	}
	if opType == OpTypeUpdate {
		return a.applyUpdate(table, intentKey, oldValues, newValues, version)
	}
	won, err := a.stampVersion(table, intentKey, version, opType == OpTypeDelete)
	if err != nil || !won {
		return false, err
	}
	switch opType {
	case OpTypeInsert, OpTypeReplace:
		return true, ApplyCDCInsert(a.tx, table, newValues)
	case OpTypeDelete:
		return true, ApplyCDCDelete(a.tx, a.schema, table, oldValues)
	default:
		return false, fmt.Errorf("unsupported operation type for CDC: %v", opType)
	}
}

// applyUpdate applies an UPDATE whose new image's key is newKey.
// The new key is stamped at version. If the UPDATE moved the row to another
// primary key, the old key is also tombstoned at version, and each key's
// outcome is applied on its own: with both stamps won the row is updated
// in place; with only the old key's won the old row is removed; with only
// the new key's won the after-image is written there. An update in place
// that matches no row inserts the after-image. It reports whether any part
// of the change was applied.
func (a *versionedApplier) applyUpdate(table string, newKey []byte, oldValues, newValues map[string][]byte, version hlc.Timestamp) (bool, error) {
	primaryKeys, err := a.schema.GetPrimaryKeys(table)
	if err != nil {
		return false, fmt.Errorf("ApplyCDCUpdate %s: failed to get primary keys: %w", table, err)
	}
	newWon, err := a.stampVersion(table, newKey, version, false)
	if err != nil {
		return false, err
	}
	oldWon := newWon
	if primaryKeyMoved(primaryKeys, oldValues, newValues) {
		oldKey, err := pkKeyFromValues(table, primaryKeys, oldValues, newValues)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(oldKey, newKey) {
			if oldWon, err = a.stampVersion(table, oldKey, version, true); err != nil {
				return false, err
			}
		}
	}
	switch {
	case oldWon && newWon:
		return true, updateInPlaceOrInsert(a.tx, a.schema, table, oldValues, newValues)
	case oldWon:
		return true, ApplyCDCDelete(a.tx, a.schema, table, oldValues)
	case newWon:
		return true, ApplyCDCInsert(a.tx, table, newValues)
	default:
		return false, nil
	}
}

// updateInPlaceOrInsert runs the UPDATE and, when it matches no row,
// inserts the after-image instead. Like ApplyCDCInsert's INSERT OR REPLACE,
// it replaces another row that holds the new key: the new key's stamp won,
// so that row is older than this change, and in commit order it was gone
// before it.
func updateInPlaceOrInsert(exec CDCExecutor, schema CDCSchemaProvider, table string, oldValues, newValues map[string][]byte) error {
	result, err := execCDCUpdate(exec, schema, "UPDATE OR REPLACE", table, oldValues, newValues)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil || n > 0 {
		return err
	}
	return ApplyCDCInsert(exec, table, newValues)
}

// applyEntry applies a committing transaction's captured row through
// apply. A row a newer version already owns is skipped, as on replay.
func (a *versionedApplier) applyEntry(entry *IntentEntry, commitTS hlc.Timestamp) error {
	_, err := a.apply(OpType(entry.Operation), entry.Table, entry.IntentKey, entry.OldValues, entry.NewValues, commitTS)
	return err
}

// tablePrimaryKeysSQL lists every user table's primary-key columns in key
// order; a table without a declared key lists one row with a NULL column
// (its key is the rowid).
const tablePrimaryKeysSQL = `
SELECT m.name, p.name FROM sqlite_master m
LEFT JOIN pragma_table_info(m.name) p ON p.pk > 0
WHERE m.type = 'table' AND m.name NOT LIKE 'sqlite\_%' ESCAPE '\' AND m.name NOT LIKE '\_\_marmot%' ESCAPE '\'
ORDER BY m.name, p.pk`

// tablePrimaryKeys returns every user table's primary-key column list,
// joined into one comparable string.
func tablePrimaryKeys(ctx context.Context, q schemaQuerier) (map[string]string, error) {
	rows, err := q.QueryContext(ctx, tablePrimaryKeysSQL)
	if err != nil {
		return nil, fmt.Errorf("list primary keys: %w", err)
	}
	defer rows.Close()

	keys := make(map[string]string)
	for rows.Next() {
		var table string
		var col sql.NullString
		if err := rows.Scan(&table, &col); err != nil {
			return nil, fmt.Errorf("list primary keys: %w", err)
		}
		keys[table] += col.String + "\x00"
	}
	return keys, rows.Err()
}

// reconcileRowVersions keeps the version table in step with one DDL
// statement, given every user table's primary key before and after it: a
// single table replaced by a single new one with the same key is a rename,
// and its versions move to the new name; any other removed table loses its
// versions, as does a kept table whose key columns changed, since its
// stored keys no longer name its rows. Those rows fall back to version
// zero, so the next change to each applies unconditionally. Each statement
// is reconciled alone, so a rebuild spread over several (create a copy,
// drop the original, rename the copy) drops the original's versions: the
// rebuilt table's rows start at version zero.
func reconcileRowVersions(ctx context.Context, q sqlExecQuerier, before, after map[string]string) error {
	var removed, created []string
	for name := range before {
		if _, ok := after[name]; !ok {
			removed = append(removed, name)
		}
	}
	for name := range after {
		if _, ok := before[name]; !ok {
			created = append(created, name)
		}
	}
	if len(removed) == 1 && len(created) == 1 && before[removed[0]] == after[created[0]] {
		return renameRowVersions(ctx, q, removed[0], created[0])
	}
	for name, key := range before {
		if newKey, kept := after[name]; kept && newKey == key {
			continue
		}
		if err := execRowVersionDDL(ctx, q, "DELETE FROM __marmot_row_version WHERE tbl = ?", name); err != nil {
			return err
		}
	}
	return nil
}

func execRowVersionDDL(ctx context.Context, q sqlExecQuerier, stmt string, args ...interface{}) error {
	if _, err := q.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("update row versions after DDL: %w", err)
	}
	return nil
}

const (
	// tombstonePurgeChunk is how many tombstones one purge statement
	// deletes: each chunk is its own short write, so a GC tick never holds
	// the database's single writer for long.
	tombstonePurgeChunk = 1000
	// tombstonePurgeMaxChunks bounds one GC tick's purge; the rest waits
	// for the next tick.
	tombstonePurgeMaxChunks = 100
	// purgeTombstonesSQL deletes up to a chunk of tombstones older than a
	// horizon, found through the partial tombstone index.
	purgeTombstonesSQL = `DELETE FROM __marmot_row_version WHERE (tbl, pk) IN (
	SELECT tbl, pk FROM __marmot_row_version WHERE deleted = 1 AND wall < ? LIMIT ?)`
)

// purgeTombstones deletes the tombstones no replayed change can still be
// older than. A log entry stays in a member's log at most gcMaxRetention
// after that member recorded it, and a member records an entry only at its
// commit or by pulling it from a log that still holds it, at most once
// (the applied marker stops a second time); so n members hold it at most
// n*gcMaxRetention past its commit. A tombstone older than that outlives
// every change it could have to beat. n is every member this node has
// known, REMOVED ones included, from the persisted registry
// (SetTombstoneMemberCountFunc); while it is unknown (0) nothing is purged.
// The bound assumes clock skew plus commit timeout well under
// gcMaxRetention. With unlimited retention (gcMaxRetention 0) no bound
// exists and tombstones are kept. Tombstones go in chunks
// (tombstonePurgeChunk), each its own short write. It returns how many were
// deleted.
func (tm *TransactionManager) purgeTombstones() (int64, error) {
	tm.mu.RLock()
	members := tm.memberCount
	dbName := tm.databaseName
	tm.mu.RUnlock()
	if tm.gcMaxRetention <= 0 || members == nil || dbName == SystemDatabaseName {
		return 0, nil
	}
	n := members()
	if n <= 0 {
		return 0, nil
	}
	horizon := time.Now().Add(-time.Duration(n) * tm.gcMaxRetention).UnixNano()
	var purged int64
	for chunk := 0; chunk < tombstonePurgeMaxChunks; chunk++ {
		res, err := tm.db.Exec(purgeTombstonesSQL, horizon, tombstonePurgeChunk)
		if err != nil {
			return purged, fmt.Errorf("purge row version tombstones: %w", err)
		}
		deleted, err := res.RowsAffected()
		if err != nil {
			return purged, fmt.Errorf("purge row version tombstones: %w", err)
		}
		purged += deleted
		if deleted < tombstonePurgeChunk {
			break
		}
	}
	return purged, nil
}

// renamedVersion is one version row being moved to a renamed table.
type renamedVersion struct {
	key                 []byte
	wall, logical, node int64
	deleted             bool
}

// renameRowVersions moves from's versions to to, re-encoding each intent
// key, which names its table, for the new name.
func renameRowVersions(ctx context.Context, q sqlExecQuerier, from, to string) error {
	rows, err := q.QueryContext(ctx, "SELECT pk, wall, logical, node, deleted FROM __marmot_row_version WHERE tbl = ?", from)
	if err != nil {
		return fmt.Errorf("read row versions of %s: %w", from, err)
	}
	var moved []renamedVersion
	for rows.Next() {
		var v renamedVersion
		if err := rows.Scan(&v.key, &v.wall, &v.logical, &v.node, &v.deleted); err != nil {
			rows.Close()
			return fmt.Errorf("read row versions of %s: %w", from, err)
		}
		_, pkValues, _, err := filter.DecodeIntentKey(v.key)
		if err != nil {
			rows.Close()
			return fmt.Errorf("re-key row version of %s: %w", from, err)
		}
		v.key = filter.EncodeIntentKey(to, pkValues)
		moved = append(moved, v)
	}
	iterErr := rows.Err()
	if err := rows.Close(); err != nil || iterErr != nil {
		return fmt.Errorf("read row versions of %s: %w", from, errors.Join(iterErr, err))
	}
	if err := execRowVersionDDL(ctx, q, "DELETE FROM __marmot_row_version WHERE tbl = ?", from); err != nil {
		return err
	}
	for _, v := range moved {
		if err := execRowVersionDDL(ctx, q, "INSERT INTO __marmot_row_version (tbl, pk, wall, logical, node, deleted) VALUES (?, ?, ?, ?, ?, ?)",
			to, v.key, v.wall, v.logical, v.node, v.deleted); err != nil {
			return err
		}
	}
	return nil
}

// primaryKeyMoved reports whether an UPDATE's old and new images differ in
// any primary-key column's encoded value; a column the old image lacks is
// taken from the new one, as ApplyCDCUpdate does.
func primaryKeyMoved(primaryKeys []string, oldValues, newValues map[string][]byte) bool {
	for _, col := range primaryKeys {
		oldV, ok := oldValues[col]
		if ok && !bytes.Equal(oldV, newValues[col]) {
			return true
		}
	}
	return false
}

// seedClockFromLog advances clock past the largest commit wall time
// metaStore has logged, so a node restarted with its wall clock set back
// never issues a commit timestamp at or below one it already committed or
// applied.
func seedClockFromLog(clock *hlc.Clock, metaStore MetaStore) error {
	if clock == nil || metaStore == nil {
		return nil
	}
	wall, err := metaStore.MaxCommitWall()
	if err != nil {
		return fmt.Errorf("read largest logged commit time: %w", err)
	}
	if wall > 0 {
		clock.Update(hlc.Timestamp{WallTime: wall + 1})
	}
	return nil
}
