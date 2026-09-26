package db

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
)

// AutoIncClaimTable is the hidden table holding one row per narrow
// AUTO_INCREMENT table across every user database: the current allocation
// base, keyed by (database, table).
//
// The table lives in the SYSTEM database (db.SystemDatabaseName), not in the
// user database the claim is about. A claim applied inside a pinned session
// on the user database (db/db_integration.go BeginPinnedSession ->
// db/preupdate_hook.go conn.BeginTx BEGIN IMMEDIATE) would try to take that
// same database's single SQLite writer a second time and block on its own
// BEGIN; the system database is a separate SQLite file with its own writer,
// so a claim never contends with a user database's pinned session. Two
// properties this table used to get "for free" by living inside each user
// database now come from that placement instead: DatabaseManager.ListDatabases
// never returns SystemDatabaseName, so the table is never client-visible
// through SHOW DATABASES-style listings, and DatabaseManager.TakeSnapshot /
// TakeSnapshotToDir checkpoint and ship the system database file explicitly
// on every snapshot, so the base travels with a restore without needing to be
// named per user database. The base deliberately does NOT live in Pebble: a
// restored node has no Pebble state, so a full-cluster restore would make
// every voter a yes-man at once.
//
// The "__marmot__" prefix still buys two properties unrelated to placement:
// the preupdate hook only ever captures CDC for tables present in the schema
// cache, and this table is created by raw DDL outside that cache, so it never
// enters CDC; and the prefix backs the parser guard
// (protocol/query/rules/claim_table_guard.go), which refuses client SQL that
// would write to, or change the schema of, any table under this prefix.
//
// Note for anyone tempted to use the name as a filter: SQLite's LIKE treats "_"
// as a single-character wildcard, so '__marmot__%' is broader than it looks and
// is not an exact-name test.
//
// The literal itself lives in common.AutoIncClaimTableName - the one place in
// the repo that spells it out - so protocol/query/rules can refuse client SQL
// naming this table without db importing protocol (which would cycle back
// through protocol -> protocol/query/rules -> db).
const AutoIncClaimTable = common.AutoIncClaimTableName

// autoIncClaimDDL creates the table. WITHOUT ROWID because the row is
// addressed only by (db, tbl) and a rowid would be dead weight. The
// composite primary key is what lets one system-database table serve every
// user database at once.
//
// A row keeps three floors apart, because they differ in what a COMMIT may
// be refused for (ApplyClaims):
//   - committed: the end of the last range this node applied. Only
//     ApplyClaims advances it, and the per-node disjointness of ACKed ranges
//     rests on it alone.
//   - seed: ids this node's own rows hold - the DDL-time and restore-time
//     MAX(id) seeds, the PREPARE-time backfill, and a rename's inheritance.
//   - merged: bases peers reported - the vote-hold merge, the membership
//     backstop, the sync command and a snapshot restore.
//
// A table's base, the value PREPARE votes against and every reader reports,
// is the largest of the three (autoIncBaseExpr). The column order is the one
// migrateAutoIncClaimTable leaves a table from before the split in.
var autoIncClaimDDL = `CREATE TABLE IF NOT EXISTS ` + AutoIncClaimTable + ` (
	db         TEXT    NOT NULL,
	tbl        TEXT    NOT NULL,
	committed  INTEGER NOT NULL,
	owner      INTEGER NOT NULL,
	granted_at INTEGER NOT NULL,
	seed       INTEGER NOT NULL DEFAULT 0,
	merged     INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (db, tbl)
) WITHOUT ROWID`

// autoIncBaseExpr is a claim row's base: the largest of its three floors.
const autoIncBaseExpr = "MAX(committed, seed, merged)"

// claimTableQuerier is what inspecting and migrating the claim table needs.
// *sql.DB and *sql.Tx both satisfy it.
type claimTableQuerier interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// autoIncClaimTableIsPreSplit reports whether the claim table in schema
// ("main", or an attached database's name) still has the single base column
// of a binary from before the split into committed, seed and merged.
func autoIncClaimTableIsPreSplit(q claimTableQuerier, schema string) (bool, error) {
	var n int
	if err := q.QueryRow("SELECT COUNT(*) FROM pragma_table_info(?, ?) WHERE name = 'base'",
		AutoIncClaimTable, schema).Scan(&n); err != nil {
		return false, fmt.Errorf("inspect %s.%s: %w", schema, AutoIncClaimTable, err)
	}
	return n > 0, nil
}

// migrateAutoIncClaimTable splits a claim table from before the split: the
// base column becomes committed, and seed and merged start at that base.
//
// A pre-split base B was the largest of everything the three floors now keep
// apart, so B bounds each of them, and giving all three B keeps every check
// this node made before the upgrade exactly as strict: committed = B still
// covers every range the node applied, which is all the disjointness argument
// needs of it. The caller runs it inside the transaction that opens the
// table, so no crash leaves a half-migrated table. A split table is left as
// it is.
func migrateAutoIncClaimTable(q claimTableQuerier) error {
	preSplit, err := autoIncClaimTableIsPreSplit(q, "main")
	if err != nil || !preSplit {
		return err
	}
	for _, stmt := range []string{
		"ALTER TABLE " + AutoIncClaimTable + " RENAME COLUMN base TO committed",
		"ALTER TABLE " + AutoIncClaimTable + " ADD COLUMN seed INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE " + AutoIncClaimTable + " ADD COLUMN merged INTEGER NOT NULL DEFAULT 0",
		"UPDATE " + AutoIncClaimTable + " SET seed = committed, merged = committed",
	} {
		if _, err := q.Exec(stmt); err != nil {
			return fmt.Errorf("split %s: %w", AutoIncClaimTable, err)
		}
	}
	return nil
}

// AutoIncClaim is the payload a claim statement carries in its intent's
// DataSnapshot. It is defined in protocol (protocol/autoinc_claim.go), which
// coordinator also depends on, because db already imports coordinator (for
// coordinator.Replicator) and coordinator importing db back would cycle. This
// alias lets db's own claim-handling code keep using the unqualified name.
type AutoIncClaim = protocol.AutoIncClaim

// ErrAutoIncBaseAbsent reports that a table has no claim row.
//
// It is an error and never a zero, and the distinction is the protocol's whole
// safety argument (the PREPARE handler backfills from the table itself rather
// than read absence as 0, see backfillAutoIncBase). The natural implementation is "absent means 0 means yes",
// and an engineer will write that unless told not to: a cache that defaults to
// 0 and accepts is precisely the thing that cannot cast the rejection that
// would repair it.
var ErrAutoIncBaseAbsent = errors.New("no auto-increment claim row for table")

// ErrAutoIncClaimNotApplicable is protocol.ErrAutoIncClaimNotApplicable,
// which ApplyClaims returns for a COMMIT whose claim this node cannot apply.
var ErrAutoIncClaimNotApplicable = protocol.ErrAutoIncClaimNotApplicable

// AutoIncClaimStore is the node's AUTO_INCREMENT base store: one row per
// (database, table). The receiver is ALWAYS the SYSTEM database, never a user
// database, and every method therefore names its database explicitly.
type AutoIncClaimStore struct {
	sys         *ReplicatedDatabase
	incarnation atomic.Pointer[AutoIncIncarnationListener]
}

// NewAutoIncClaimStore wraps the system database as a claim store. sys must
// be the system database (db.SystemDatabaseName), never a user database.
func NewAutoIncClaimStore(sys *ReplicatedDatabase) *AutoIncClaimStore {
	return &AutoIncClaimStore{sys: sys}
}

// ReadBase returns a table's base: the largest of its committed, seed and
// merged floors, which is what PREPARE votes against.
//
// It reads through the READ pool, not the write handle: writeDB is capped at a
// single connection, so reading a base through it would put every claim behind
// the one SQLite writer.
//
// An absent row returns ErrAutoIncBaseAbsent. Callers must reject on it, and
// on every other error too. The claim table is created with the system
// database (DatabaseManager's system-database initialisation), so a missing
// table is not an expected state and surfaces as the read error it is.
func (s *AutoIncClaimStore) ReadBase(database, table string) (uint64, error) {
	var base int64
	err := s.sys.GetReadDB().QueryRow(
		"SELECT "+autoIncBaseExpr+" FROM "+AutoIncClaimTable+" WHERE db = ? AND tbl = ?", database, table).Scan(&base)
	switch {
	case err == sql.ErrNoRows:
		return 0, ErrAutoIncBaseAbsent
	case err != nil:
		return 0, fmt.Errorf("read auto-increment base for %s.%s: %w", database, table, err)
	}
	if base < 0 {
		return 0, fmt.Errorf("auto-increment base for %s.%s is negative (%d)", database, table, base)
	}
	return uint64(base), nil
}

// Seed creates or RAISES a table's seed floor to floor, the largest id this
// node's own rows hold for it (or the declared floor). Never lowers. It runs
// at DDL time, when a restored database is reattached, and when PREPARE
// backfills an absent row.
//
// Tagging an existing table holding ids 1..1000 with an absent row would leave
// the base at 0, so the first range would be [0,R) and the allocator would
// mint 1, 2, 3 over live rows: a UNIQUE failure on a node that holds them and
// a silent overwrite on one that does not. Each node seeds from its own
// MAX(id) when it applies the replicated DDL, so seeds can differ; they
// converge upward on the first claim, because the PREPARE condition rejects
// any proposal at or below a participant's own stored base and returns that
// base for the retry.
func (s *AutoIncClaimStore) Seed(database, table string, floor, owner uint64) error {
	writeDB := s.sys.GetWriteDB()
	if _, err := writeDB.Exec(autoIncClaimDDL); err != nil {
		return fmt.Errorf("create %s: %w", AutoIncClaimTable, err)
	}
	_, err := writeDB.Exec(
		"INSERT INTO "+AutoIncClaimTable+" (db, tbl, committed, seed, merged, owner, granted_at) VALUES (?, ?, 0, ?, 0, ?, ?) "+
			"ON CONFLICT(db, tbl) DO UPDATE SET seed = MAX(seed, excluded.seed), owner = excluded.owner, granted_at = excluded.granted_at",
		database, table, int64(floor), int64(owner), time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("seed auto-increment base for %s.%s: %w", database, table, err)
	}
	return nil
}

// Inherit raises table's seed floor to at least the base of every table in
// from, creating its row if needed, and keeps the rows of from. It runs when
// one DDL statement created table while removing the tables in from (a
// RENAME): table may now hold their rows, and so ids granted under their
// names. Every range granted under a name ends at or below that name's base
// on a majority, so after Inherit every later grant for table lies above
// every id table can hold. Never lowers, and never deletes a row: a base is
// monotone per (database, table) name for the life of the cluster.
//
// It raises the seed floor, not the merged one: the rows came into this
// node's own table inside the DDL, exactly as the MAX(id) seed that runs
// after it records, so a claim on table PREPARED before the rename and
// committed after it is refused like one a DDL seed overtook.
func (s *AutoIncClaimStore) Inherit(database, table string, from []string, owner uint64) (err error) {
	tx, err := s.sys.GetWriteDB().Begin()
	if err != nil {
		return fmt.Errorf("begin auto-increment base inheritance for %s.%s: %w", database, table, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UnixNano()
	for _, f := range from {
		if _, err = tx.Exec("INSERT INTO "+AutoIncClaimTable+" (db, tbl, committed, seed, merged, owner, granted_at) "+
			"SELECT db, ?, 0, "+autoIncBaseExpr+", 0, ?, ? FROM "+AutoIncClaimTable+" WHERE db = ? AND tbl = ? "+
			"ON CONFLICT(db, tbl) DO UPDATE SET seed = excluded.seed, owner = excluded.owner, granted_at = excluded.granted_at "+
			"WHERE excluded.seed > seed",
			table, int64(owner), now, database, f); err != nil {
			return fmt.Errorf("raise auto-increment base of %s.%s to %s's: %w", database, table, f, err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit auto-increment base inheritance for %s.%s: %w", database, table, err)
	}
	return nil
}

// AutoIncIncarnationListener learns of every table incarnation a DDL
// statement ends on this node, before the new definitions become visible to
// queries: a table dropped, created, renamed from or to a name, or redefined
// (TableIncarnationEnded), and every table of a dropped database
// (DatabaseIncarnationEnded). The node's narrow allocator discards its
// in-memory ranges for them. Such a range was granted for the old
// incarnation; it is disjoint from every later grant under the same name,
// but the rows of a new incarnation may come from grants under another name
// (RENAME).
type AutoIncIncarnationListener interface {
	TableIncarnationEnded(database, table string)
	DatabaseIncarnationEnded(database string)
}

// SetIncarnationListener registers l (AutoIncIncarnationListener).
func (s *AutoIncClaimStore) SetIncarnationListener(l AutoIncIncarnationListener) {
	s.incarnation.Store(&l)
}

// tableIncarnationEnded reports table to the registered listener, if any.
func (s *AutoIncClaimStore) tableIncarnationEnded(database, table string) {
	if l := s.incarnation.Load(); l != nil {
		(*l).TableIncarnationEnded(database, table)
	}
}

// databaseIncarnationEnded reports database to the registered listener, if
// any.
func (s *AutoIncClaimStore) databaseIncarnationEnded(database string) {
	if l := s.incarnation.Load(); l != nil {
		(*l).DatabaseIncarnationEnded(database)
	}
}

// RaiseAutoIncBasesFrom prepares the peer's system database file at
// incomingPath to become this node's: it keeps every floor the system
// database file at localPath holds for the same (database, table), and copies
// in any row only localPath has. It is run on a peer's system database before
// a restore installs it in place of this node's own.
//
// A restore must never lower a base this node committed. The peer may have
// missed a claim this node was in the majority for; installing the peer's
// lower base would let this node accept that range again, and majority
// intersection - the protocol's whole safety argument - would be gone.
//
// Where each floor lands (see autoIncClaimDDL):
//   - the peer's committed and merged floors become merged: they record
//     ranges other nodes applied, which is what a merge carries, and a merge
//     never refuses a COMMIT;
//   - the peer's seed stays seed: it counts ids the peer's rows hold, and the
//     snapshot this file travels with makes those rows this node's own;
//   - this node's own committed, seed and merged floors are kept, each as the
//     larger of the two, so committed stays exactly this node's.
//
// A peer file from before the split holds one base B, which cannot be taken
// apart, so B becomes all three floors (migrateAutoIncClaimTable) and only its
// committed moves to merged: B, the peer's committed included, stays in seed.
// That is the conservative choice, and it costs liveness only: during a rolling
// upgrade, a claim this node holds PREPAREd across such a restore can have its
// COMMIT refused until the pending-transaction GC.
//
// The vote hold (AutoIncHoldTable) is this node's, never the peer's: the
// incoming file keeps the local file's hold. A missing localPath - a node with
// no claim history of its own, which may nonetheless have ACKed claims before
// it lost its data - adds nothing to the peer's floors and HOLDS this node's
// votes until it has merged bases from a majority.
func RaiseAutoIncBasesFrom(incomingPath, localPath string) (err error) {
	hasLocal := true
	if _, statErr := os.Stat(localPath); errors.Is(statErr, os.ErrNotExist) {
		hasLocal = false
	} else if statErr != nil {
		return fmt.Errorf("stat local system database: %w", statErr)
	}

	// The merged bases must survive a power loss once the restorer moves this
	// file into place, as every other system-database commit does
	// (WithDurableCommits): the durable driver, with synchronous=FULL for the
	// checkpoint that lands them in the main file.
	conn, err := sql.Open(SQLiteDurableDriverName, incomingPath+"?_sync=FULL")
	if err != nil {
		return fmt.Errorf("open incoming system database: %w", err)
	}
	defer func() {
		if closeErr := conn.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close incoming system database: %w", closeErr)
		}
	}()
	// ATTACH is per connection, so every statement below must run on one.
	conn.SetMaxOpenConns(1)

	if err := adoptPeerClaimTable(conn); err != nil {
		return err
	}
	if _, err := conn.Exec(autoIncHoldDDL); err != nil {
		return fmt.Errorf("create %s in incoming system database: %w", AutoIncHoldTable, err)
	}
	if _, err := conn.Exec("DELETE FROM main." + AutoIncHoldTable); err != nil {
		return fmt.Errorf("clear the peer's vote hold: %w", err)
	}
	if !hasLocal {
		if _, err := conn.Exec(holdVotesSQL, time.Now().UnixNano()); err != nil {
			return fmt.Errorf("hold auto-increment votes: %w", err)
		}
		return nil
	}
	if _, err := conn.Exec("ATTACH DATABASE ? AS local", localPath); err != nil {
		return fmt.Errorf("attach local system database: %w", err)
	}
	var localHasTable int
	if err := conn.QueryRow("SELECT COUNT(*) FROM local.sqlite_master WHERE type = 'table' AND name = ?",
		AutoIncClaimTable).Scan(&localHasTable); err != nil {
		return fmt.Errorf("inspect local system database: %w", err)
	}
	var localHasHold int
	if err := conn.QueryRow("SELECT COUNT(*) FROM local.sqlite_master WHERE type = 'table' AND name = ?",
		AutoIncHoldTable).Scan(&localHasHold); err != nil {
		return fmt.Errorf("inspect local system database: %w", err)
	}
	if localHasHold > 0 {
		if _, err := conn.Exec("INSERT INTO main." + AutoIncHoldTable + " (id, since) SELECT id, since FROM local." + AutoIncHoldTable); err != nil {
			return fmt.Errorf("keep this node's vote hold: %w", err)
		}
	}
	if localHasTable > 0 {
		// The local file is only read: a binary from before the split wrote
		// one base, which bounds all three floors (migrateAutoIncClaimTable).
		localPreSplit, err := autoIncClaimTableIsPreSplit(conn, "local")
		if err != nil {
			return err
		}
		floors := "committed, seed, merged"
		if localPreSplit {
			floors = "base, base, base"
		}
		// "WHERE true" disambiguates the upsert's ON CONFLICT from a join
		// constraint, as SQLite requires for INSERT ... SELECT.
		if _, err := conn.Exec("INSERT INTO main." + AutoIncClaimTable + " (db, tbl, committed, seed, merged, owner, granted_at) " +
			"SELECT db, tbl, " + floors + ", owner, granted_at FROM local." + AutoIncClaimTable + " WHERE true " +
			"ON CONFLICT(db, tbl) DO UPDATE SET committed = MAX(committed, excluded.committed), " +
			"seed = MAX(seed, excluded.seed), merged = MAX(merged, excluded.merged), " +
			"owner = excluded.owner, granted_at = excluded.granted_at"); err != nil {
			return fmt.Errorf("keep this node's auto-increment floors: %w", err)
		}
	}
	if _, err := conn.Exec("DETACH DATABASE local"); err != nil {
		return fmt.Errorf("detach local system database: %w", err)
	}
	// The restorer moves only the main database file into place. Closing the
	// last connection checkpoints the write-ahead log into it; a failed close
	// is returned by the deferred Close above.
	return nil
}

// adoptPeerClaimTable readies the claim table of the peer's system database
// on conn to become this node's, in one transaction: it creates the table if
// the peer's binary predates it (the local rows still have to land), splits a
// table from before the split, and moves the peer's committed floor into
// merged. A pre-split table's base also stays in seed, as the split gives it
// to all three floors (RaiseAutoIncBasesFrom).
func adoptPeerClaimTable(conn *sql.DB) (err error) {
	tx, err := conn.Begin()
	if err != nil {
		return fmt.Errorf("begin adopting the peer's %s: %w", AutoIncClaimTable, err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	if _, err = tx.Exec(autoIncClaimDDL); err != nil {
		return fmt.Errorf("create %s in incoming system database: %w", AutoIncClaimTable, err)
	}
	if err = migrateAutoIncClaimTable(tx); err != nil {
		return err
	}
	if _, err = tx.Exec("UPDATE " + AutoIncClaimTable + " SET merged = MAX(committed, merged), committed = 0"); err != nil {
		return fmt.Errorf("record the peer's committed floors as merged: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit adopting the peer's %s: %w", AutoIncClaimTable, err)
	}
	return nil
}

// AutoIncWidthMax returns the largest id the table's AUTO_INCREMENT column can
// hold, derived from THIS node's own schema.
//
// It is never taken from a claimant. The width marker rides in the replicated
// DDL text, so every node derives the same ceiling from the same source;
// trusting the proposer would let a node whose schema is mid-DDL rubber-stamp a
// range its column cannot hold.
//
// A table with no width marker has no narrow ceiling, and a claim against it is
// a caller error rather than a width question: the narrow allocator is only
// ever engaged for a marked column.
//
// This stays a method on the USER database: widthMax is derived from the user
// database's own schema, a rule the relocation of the claim store to the
// system database does not touch.
func (mdb *ReplicatedDatabase) AutoIncWidthMax(table string) (uint64, error) {
	schema, err := mdb.GetCachedTableSchema(table)
	if err != nil {
		return 0, fmt.Errorf("no cached schema for %s: %w", table, err)
	}
	autoIncCol := schema.GetAutoIncrementCol()
	if autoIncCol == "" {
		return 0, fmt.Errorf("table %s has no auto-increment column", table)
	}
	for _, col := range schema.FullColumns {
		if !strings.EqualFold(col.Name, autoIncCol) {
			continue
		}
		attrs := intmarker.Attributes{Bits: col.DeclaredWidth, Unsigned: col.Unsigned}
		if !attrs.Marked() {
			return 0, fmt.Errorf("table %s auto-increment column %s carries no width marker", table, autoIncCol)
		}
		return attrs.WidthMax(), nil
	}
	return 0, fmt.Errorf("table %s auto-increment column %s is absent from its schema", table, autoIncCol)
}

// ApplyClaims writes the committed floor for every AUTO_INCREMENT range claim
// prepared under txnID against database. It is called by the COMMIT handler,
// on the user-database path, only for a COMMIT that carries the claim flag,
// and before the transaction is committed.
//
// meta is the USER database's meta store: the transaction whose intents are
// being read is against the user database, even though the write this method
// performs lands in the system database.
//
// The committed floor becomes newBase + size (autoIncClaimDDL). The claimant
// owns newBase+1 .. newBase+size, so newBase+size is both the last id handed
// out and the prevBase the next claimant proposes.
//
// The write is conditional on the committed and seed floors both being at or
// below the claim's newBase, where its range starts, and ApplyClaims fails
// with ErrAutoIncClaimNotApplicable when either is not, or when the
// transaction holds no claim intent at all. The merged floor is not tested.
//
// Why the ranges one node ACKs are pairwise disjoint, from committed alone:
//  1. After the row is created (committed 0, or a pre-split base on upgrade),
//     only this method writes committed, and a restore keeps this node's own
//     committed (RaiseAutoIncBasesFrom). The write happens only where
//     committed <= newBase and sets newBase+size > newBase, since size > 0:
//     committed never decreases.
//  2. So once this node applied (n, n+s], committed >= n+s from then on.
//  3. The system database has one writer, so applies are serialised. A later
//     apply of (n', n'+s'] needs committed <= n' at that moment, so
//     n' >= n+s: it starts at or above the end of every earlier range.
//  4. Two overlapping ranges can therefore not both gather a commit majority:
//     any two majorities share a node, and it would have applied both.
//
// That argument does not trust the claim key's row lock to have survived from
// PREPARE to COMMIT. The one way it does not - a stale-transaction abort
// racing a COMMIT that already read its intent - lives inside one call of
// this method, during which committed can only rise. While the lock does hold
// (a pending holder is never overwritten, and it persists across a restart),
// nothing else is applied for the table between this claim's PREPARE and its
// COMMIT, and PREPARE voted against the largest of all three floors.
//
// A merged floor is left out because it records ranges other nodes applied.
// A peer that committed this same claim first reports exactly newBase+size,
// so testing it would refuse the claim this node voted for; and a range
// overlapping this claim that another majority committed is excluded by 4.
// Merged does its work at PREPARE: a node that lost its claim state votes
// again only after its merged floor covers every claim it may have ACKed
// before (grpc autoIncMergeSafe), so every PREPARE it answers sees them.
//
// The seed floor counts ids this node's own rows hold. A seed raised between
// PREPARE and COMMIT - a DDL, a rename's inheritance, a reattach after
// restore - refuses a range that would sit below ids now in the table.
//
// Every claim in the transaction lands in ONE SQLite transaction against the
// system database. A partially applied set would leave this node holding a
// base below a range it had already voted to give away, and the PREPARE condition
// would then let it accept that same range again.
//
// The values come only from the intents. A claim's DataSnapshot was fixed at
// PREPARE, when this node checked it against its own schema and its own stored
// base; the COMMIT message is decision metadata and carries no payload to read.
// Owner and grant time are taken from the intent record's own NodeID and TSWall
// rather than from the payload, so a claimant cannot misattribute a range to
// another node.
//
// The row is updated in place and never deleted, by this or any DDL: a base
// is monotone per (database, table) name for the life of the cluster, and a
// removed row would let a name be granted a range it was granted before.
func (s *AutoIncClaimStore) ApplyClaims(database string, txnID uint64, meta MetaStore) error {
	intents, err := meta.GetIntentsByTxn(txnID)
	if err != nil {
		return fmt.Errorf("read intents for txn %d: %w", txnID, err)
	}

	tx, err := s.sys.GetWriteDB().Begin()
	if err != nil {
		return fmt.Errorf("begin auto-increment claim apply: %w", err)
	}
	applied := 0
	for _, intent := range intents {
		if intent.IntentType != IntentTypeAutoIDClaim {
			continue
		}
		if aErr := applyClaimTx(tx, database, intent); aErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("txn %d: %w", txnID, aErr)
		}
		applied++
	}
	if applied == 0 {
		_ = tx.Rollback()
		return fmt.Errorf("txn %d: %w: no claim intent is held for this transaction", txnID, ErrAutoIncClaimNotApplicable)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit auto-increment claim apply for txn %d: %w", txnID, err)
	}
	return nil
}

// applyClaimTx applies one claim intent inside tx, conditional on neither the
// committed nor the seed floor having passed the claim's newBase (see
// ApplyClaims).
func applyClaimTx(tx *sql.Tx, database string, intent *WriteIntentRecord) error {
	claim, err := protocol.DecodeAutoIncClaim(intent.DataSnapshot)
	if err != nil {
		return err
	}
	// Re-checked here, not assumed from PREPARE: this arithmetic decides what
	// is written, and a wrapped sum would store a base below the ids the range
	// just handed out. The base column is a signed SQLite INTEGER, so the sum
	// must also fit in an int64.
	if claim.Size == 0 || claim.NewBase > math.MaxInt64 || claim.Size > math.MaxInt64-claim.NewBase {
		return fmt.Errorf("auto-increment claim for %s has an unusable size %d at base %d",
			claim.Table, claim.Size, claim.NewBase)
	}
	res, err := tx.Exec(
		"UPDATE "+AutoIncClaimTable+" SET committed = ?, owner = ?, granted_at = ? "+
			"WHERE db = ? AND tbl = ? AND committed <= ? AND seed <= ?",
		int64(claim.NewBase+claim.Size), int64(intent.NodeID), intent.TSWall, database, claim.Table,
		int64(claim.NewBase), int64(claim.NewBase))
	if err != nil {
		return fmt.Errorf("write auto-increment base for %s.%s: %w", database, claim.Table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("write auto-increment base for %s.%s: %w", database, claim.Table, err)
	}
	if n != 1 {
		return fmt.Errorf("%w: the claim row for %s.%s is absent or its committed or seed floor has passed the claim's newBase %d",
			ErrAutoIncClaimNotApplicable, database, claim.Table, claim.NewBase)
	}
	return nil
}
