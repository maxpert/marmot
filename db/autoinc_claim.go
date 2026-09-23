package db

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
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
var autoIncClaimDDL = `CREATE TABLE IF NOT EXISTS ` + AutoIncClaimTable + ` (
	db         TEXT    NOT NULL,
	tbl        TEXT    NOT NULL,
	base       INTEGER NOT NULL,
	owner      INTEGER NOT NULL,
	granted_at INTEGER NOT NULL,
	PRIMARY KEY (db, tbl)
) WITHOUT ROWID`

// AutoIncClaim is the payload a claim statement carries in its intent's
// DataSnapshot. It is defined in protocol (protocol/autoinc_claim.go), which
// coordinator also depends on, because db already imports coordinator (for
// coordinator.Replicator) and coordinator importing db back would cycle. This
// alias lets db's own claim-handling code keep using the unqualified name.
type AutoIncClaim = protocol.AutoIncClaim

// ErrAutoIncBaseAbsent reports that a table has no claim row.
//
// It is an error and never a zero, and the distinction is the protocol's whole
// safety argument. The natural implementation is "absent means 0 means yes",
// and an engineer will write that unless told not to: a cache that defaults to
// 0 and accepts is precisely the thing that cannot cast the rejection that
// would repair it.
var ErrAutoIncBaseAbsent = errors.New("no auto-increment claim row for table")

// ErrAutoIncClaimNotApplicable reports a COMMIT whose claim this node cannot
// apply: the claim intent written at PREPARE is gone, or the stored base has
// moved past the claim's newBase since PREPARE. Either way this node did not
// hold the claim continuously from its vote to its commit, and ACKing would
// let it count toward a quorum for a range it may also have granted to
// another claimant.
var ErrAutoIncClaimNotApplicable = errors.New("auto-increment claim cannot be applied")

// AutoIncClaimStore is the node's AUTO_INCREMENT base store: one row per
// (database, table). The receiver is ALWAYS the SYSTEM database, never a user
// database, and every method therefore names its database explicitly.
type AutoIncClaimStore struct {
	sys *ReplicatedDatabase
}

// NewAutoIncClaimStore wraps the system database as a claim store. sys must
// be the system database (db.SystemDatabaseName), never a user database.
func NewAutoIncClaimStore(sys *ReplicatedDatabase) *AutoIncClaimStore {
	return &AutoIncClaimStore{sys: sys}
}

// ReadBase returns a table's committed base.
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
		"SELECT base FROM "+AutoIncClaimTable+" WHERE db = ? AND tbl = ?", database, table).Scan(&base)
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

// Seed creates or RAISES a table's base at DDL time. Never lowers.
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
		"INSERT INTO "+AutoIncClaimTable+" (db, tbl, base, owner, granted_at) VALUES (?, ?, ?, ?, ?) "+
			"ON CONFLICT(db, tbl) DO UPDATE SET base = MAX(base, excluded.base), owner = excluded.owner, granted_at = excluded.granted_at",
		database, table, int64(floor), int64(owner), time.Now().UnixNano())
	if err != nil {
		return fmt.Errorf("seed auto-increment base for %s.%s: %w", database, table, err)
	}
	return nil
}

// RaiseAutoIncBasesFrom raises every claim base in the system database file at
// incomingPath to at least the base the system database file at localPath
// holds for the same (database, table), and copies in any row only localPath
// has. It is run on a peer's system database before a restore installs it in
// place of this node's own.
//
// A restore must never lower a base this node committed. The peer may have
// missed a claim this node was in the majority for; installing the peer's
// lower base would let this node accept that range again, and majority
// intersection - the protocol's whole safety argument - would be gone. A
// missing localPath (a node with no prior state) leaves incomingPath as it is.
func RaiseAutoIncBasesFrom(incomingPath, localPath string) (err error) {
	if _, statErr := os.Stat(localPath); errors.Is(statErr, os.ErrNotExist) {
		return nil
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

	// A peer running a binary older than the claim table ships a system
	// database without it; the local rows still have to land.
	if _, err := conn.Exec(autoIncClaimDDL); err != nil {
		return fmt.Errorf("create %s in incoming system database: %w", AutoIncClaimTable, err)
	}
	if _, err := conn.Exec("ATTACH DATABASE ? AS local", localPath); err != nil {
		return fmt.Errorf("attach local system database: %w", err)
	}
	var localHasTable int
	if err := conn.QueryRow("SELECT COUNT(*) FROM local.sqlite_master WHERE type = 'table' AND name = ?",
		AutoIncClaimTable).Scan(&localHasTable); err != nil {
		return fmt.Errorf("inspect local system database: %w", err)
	}
	if localHasTable > 0 {
		// "WHERE true" disambiguates the upsert's ON CONFLICT from a join
		// constraint, as SQLite requires for INSERT ... SELECT.
		if _, err := conn.Exec("INSERT INTO main." + AutoIncClaimTable + " (db, tbl, base, owner, granted_at) " +
			"SELECT db, tbl, base, owner, granted_at FROM local." + AutoIncClaimTable + " WHERE true " +
			"ON CONFLICT(db, tbl) DO UPDATE SET base = excluded.base, owner = excluded.owner, granted_at = excluded.granted_at " +
			"WHERE excluded.base > base"); err != nil {
			return fmt.Errorf("raise incoming auto-increment bases: %w", err)
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

// ApplyClaims writes the committed base for every AUTO_INCREMENT range claim
// prepared under txnID against database. It is called by the COMMIT handler,
// on the user-database path, only for a COMMIT that carries the claim flag,
// and before the transaction is committed.
//
// meta is the USER database's meta store: the transaction whose intents are
// being read is against the user database, even though the write this method
// performs lands in the system database.
//
// The stored base becomes newBase + size. The claimant owns
// newBase+1 .. newBase+size, so newBase+size is both the
// last id handed out and the prevBase the next claimant proposes. That sum is
// the quantity the protocol advances monotonically.
//
// The write is conditional on the stored base still being at or below the
// claim's newBase, where its range starts, and ApplyClaims fails with ErrAutoIncClaimNotApplicable
// when it is not, or when the transaction holds no claim intent at all. This
// is what makes a node's COMMIT ACKs safe without trusting the claim key's
// row lock to have survived from PREPARE to COMMIT: the system database has
// one writer, so the conditional writes a node applies are serialised, and
// each applies only if no range it already committed reaches past the new
// claim's newBase. The condition is on newBase rather than prevBase so that it
// holds on its own, not only because PREPARE admitted newBase == prevBase. The ranges one node ACKs are therefore pairwise disjoint,
// and two overlapping ranges cannot both gather a commit majority, because
// any two majorities share a node. The same condition keeps a base that a DDL
// seed raised between PREPARE and COMMIT from being lowered, and refuses a
// range that would sit below that raised floor.
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
// The row is updated in place and never deleted: an absent row is a hard rejection at PREPARE, so any code that
// removed rows would create a window in which a live table re-mints ids it has
// already used.
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

// applyClaimTx applies one claim intent inside tx, conditional on the stored
// base not having passed the claim's newBase (see ApplyClaims).
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
		"UPDATE "+AutoIncClaimTable+" SET base = ?, owner = ?, granted_at = ? WHERE db = ? AND tbl = ? AND base <= ?",
		int64(claim.NewBase+claim.Size), int64(intent.NodeID), intent.TSWall, database, claim.Table, int64(claim.NewBase))
	if err != nil {
		return fmt.Errorf("write auto-increment base for %s.%s: %w", database, claim.Table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("write auto-increment base for %s.%s: %w", database, claim.Table, err)
	}
	if n != 1 {
		return fmt.Errorf("%w: the stored base for %s.%s is absent or has passed the claim's newBase %d",
			ErrAutoIncClaimNotApplicable, database, claim.Table, claim.NewBase)
	}
	return nil
}
