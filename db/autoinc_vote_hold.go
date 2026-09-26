package db

import (
	"database/sql"
	"fmt"
	"math"
	"time"

	"github.com/maxpert/marmot/common"
)

// AutoIncHoldTable holds this node's AUTO_INCREMENT claim votes. While its
// single row is present the node declines every claim at PREPARE, without a
// verdict, until it has raised its claim bases to the maximum reported by a
// majority of its peers (MergeAndReleaseVotes).
//
// The claim protocol's safety rests on majority intersection: every
// committed claim reached a majority, and each of those nodes keeps a base at
// or above the claim's end for as long as it votes. A node that lost its
// claim state breaks that premise, and every way of losing it is held:
//   - a system database this process initialises (createAutoIncTables): the
//     node is new, or lost its data directory or system database, and cannot
//     tell which;
//   - a system database that arrived from a peer with no local file to merge
//     (RaiseAutoIncBasesFrom): the node may have ACKed claims whose rows the
//     peer lacks.
//
// Claims never enter CDC, so catching up gives such a node no base above its
// DDL-time seed; only the merge (grpc.RunAutoIncBaseMerge) does.
//
// The row lives in the system database beside the bases it qualifies, so a
// hold survives a restart until the merge completes, and the merge that
// raises the bases releases the hold in the same transaction.
const AutoIncHoldTable = common.AutoIncHoldTableName

var autoIncHoldDDL = `CREATE TABLE IF NOT EXISTS ` + AutoIncHoldTable + ` (
	id    INTEGER PRIMARY KEY CHECK (id = 1),
	since INTEGER NOT NULL
)`

// holdVotesSQL places the hold; it is idempotent.
var holdVotesSQL = "INSERT OR IGNORE INTO " + AutoIncHoldTable + " (id, since) VALUES (1, ?)"

// AutoIncBase is one table's allocation base: the largest of its committed,
// seed and merged floors (autoIncClaimDDL).
type AutoIncBase struct {
	Database string
	Table    string
	Base     uint64
}

// VotesHeld reports whether this node's claim votes are held.
func (s *AutoIncClaimStore) VotesHeld() (bool, error) {
	var n int
	if err := s.sys.GetReadDB().QueryRow("SELECT COUNT(*) FROM " + AutoIncHoldTable).Scan(&n); err != nil {
		return false, fmt.Errorf("read %s: %w", AutoIncHoldTable, err)
	}
	return n > 0, nil
}

// Bases returns every base this node holds, as a peer's merge reads them.
//
// A base is the largest of the three floors, not committed alone. The merge's
// proof (grpc autoIncMergeSafe) needs an unheld peer's answer to cover every
// claim it ACKed - its committed floor does - or every claim the merge that
// released it covered - its merged floor does. The seed floor only raises the
// answer, which burns ids and never reissues one.
func (s *AutoIncClaimStore) Bases() ([]AutoIncBase, error) {
	rows, err := s.sys.GetReadDB().Query("SELECT db, tbl, " + autoIncBaseExpr + " FROM " + AutoIncClaimTable)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", AutoIncClaimTable, err)
	}
	defer rows.Close()

	var bases []AutoIncBase
	for rows.Next() {
		var b AutoIncBase
		var base int64
		if err := rows.Scan(&b.Database, &b.Table, &base); err != nil {
			return nil, fmt.Errorf("scan %s: %w", AutoIncClaimTable, err)
		}
		if base < 0 {
			return nil, fmt.Errorf("auto-increment base for %s.%s is negative (%d)", b.Database, b.Table, base)
		}
		b.Base = uint64(base)
		bases = append(bases, b)
	}
	return bases, rows.Err()
}

// MergeAndReleaseVotes raises every merged floor to at least the base bases
// reports for the same table, adds any table only bases names, and releases
// the vote hold, all in one transaction: the node votes again only with the
// merged bases durably in place. A floor is never lowered.
func (s *AutoIncClaimStore) MergeAndReleaseVotes(bases []AutoIncBase) error {
	return s.raiseBases(bases, true)
}

// RaiseBases raises every merged floor to at least the base bases reports
// for the same table, and adds any table only bases names, in one
// transaction. It never lowers a floor and never touches the vote hold.
// Raising is always safe: a higher merged floor only makes this node refuse
// more claims at PREPARE, and makes the claims it grants start higher. It
// never refuses a COMMIT: ApplyClaims does not test the merged floor, so a
// raise that lands between a claim's PREPARE and its COMMIT - a peer that
// committed the same claim first reports exactly its end - leaves that
// COMMIT to apply.
func (s *AutoIncClaimStore) RaiseBases(bases []AutoIncBase) error {
	return s.raiseBases(bases, false)
}

// raiseBases is RaiseBases, also releasing the vote hold in the same
// transaction when release is set.
func (s *AutoIncClaimStore) raiseBases(bases []AutoIncBase, release bool) (err error) {
	tx, err := s.sys.GetWriteDB().Begin()
	if err != nil {
		return fmt.Errorf("begin auto-increment base merge: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	now := time.Now().UnixNano()
	for _, b := range bases {
		if b.Base > math.MaxInt64 {
			return fmt.Errorf("merged auto-increment base for %s.%s is out of range (%d)", b.Database, b.Table, b.Base)
		}
		// owner 0 on a new row: it was created by a merge, not granted to a
		// node. An existing row keeps the owner of its last applied range.
		if _, err = tx.Exec("INSERT INTO "+AutoIncClaimTable+" (db, tbl, committed, seed, merged, owner, granted_at) VALUES (?, ?, 0, 0, ?, 0, ?) "+
			"ON CONFLICT(db, tbl) DO UPDATE SET merged = excluded.merged "+
			"WHERE excluded.merged > merged",
			b.Database, b.Table, int64(b.Base), now); err != nil {
			return fmt.Errorf("merge auto-increment base for %s.%s: %w", b.Database, b.Table, err)
		}
	}
	if release {
		if _, err = tx.Exec("DELETE FROM " + AutoIncHoldTable); err != nil {
			return fmt.Errorf("release auto-increment vote hold: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit auto-increment base merge: %w", err)
	}
	return nil
}

// createAutoIncTables creates the claim and hold tables on the system
// database, and holds this node's votes when the system database has never
// been initialised: it has no claim table and no registered database.
//
// Such a node is new, or lost its data directory or system database, or
// crashed during its first initialisation; it cannot tell which, and a node
// that lost its state may have ACKed claims it no longer has a base for. The
// hold therefore does not depend on seeds, on the catch-up strategy or on
// whether deciding one failed. A system database that has registered
// databases but no claim table was written by a binary that predates claims
// and so never voted; it is not held, which keeps a rolling upgrade from
// holding every node at once. The hold is placed in the same transaction
// that creates the claim table, so no crash can leave a claim table without
// the hold it was created with. A claim table from before the split into
// committed, seed and merged floors is split in the same transaction
// (migrateAutoIncClaimTable).
func createAutoIncTables(sys *sql.DB) (err error) {
	tx, err := sys.Begin()
	if err != nil {
		return fmt.Errorf("begin auto-increment table setup: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var existing, registered int
	if err = tx.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
		AutoIncClaimTable).Scan(&existing); err != nil {
		return fmt.Errorf("inspect system database: %w", err)
	}
	if err = tx.QueryRow("SELECT COUNT(*) FROM __marmot_databases").Scan(&registered); err != nil {
		return fmt.Errorf("inspect system database: %w", err)
	}
	if _, err = tx.Exec(autoIncClaimDDL); err != nil {
		return fmt.Errorf("create %s: %w", AutoIncClaimTable, err)
	}
	if err = migrateAutoIncClaimTable(tx); err != nil {
		return err
	}
	if _, err = tx.Exec(autoIncHoldDDL); err != nil {
		return fmt.Errorf("create %s: %w", AutoIncHoldTable, err)
	}
	if existing == 0 && registered == 0 {
		if _, err = tx.Exec(holdVotesSQL, time.Now().UnixNano()); err != nil {
			return fmt.Errorf("hold auto-increment votes: %w", err)
		}
	}
	return tx.Commit()
}

// AutoIncVotesHeld reports whether this node's claim votes are held.
func (dm *DatabaseManager) AutoIncVotesHeld() (bool, error) {
	return NewAutoIncClaimStore(dm.systemDB).VotesHeld()
}

// AutoIncBases returns every claim base this node holds (Bases), and
// whether its own votes are held - in which case a merging peer must not
// count it.
func (dm *DatabaseManager) AutoIncBases() ([]AutoIncBase, bool, error) {
	store := NewAutoIncClaimStore(dm.systemDB)
	held, err := store.VotesHeld()
	if err != nil {
		return nil, false, err
	}
	bases, err := store.Bases()
	if err != nil {
		return nil, false, err
	}
	return bases, held, nil
}

// MergeAutoIncBasesAndReleaseVotes raises this node's merged floors to at
// least bases and releases its vote hold, atomically.
func (dm *DatabaseManager) MergeAutoIncBasesAndReleaseVotes(bases []AutoIncBase) error {
	return NewAutoIncClaimStore(dm.systemDB).MergeAndReleaseVotes(bases)
}

// RaiseAutoIncBases raises this node's merged floors to at least bases, in
// one transaction, without touching its vote hold (RaiseBases).
func (dm *DatabaseManager) RaiseAutoIncBases(bases []AutoIncBase) error {
	return NewAutoIncClaimStore(dm.systemDB).RaiseBases(bases)
}
