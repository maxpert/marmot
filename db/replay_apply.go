package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/hlc"
	"github.com/rs/zerolog/log"
)

// reapplyLocalLogPageSize bounds how many local log entries ReapplyLocalLog
// lists from MetaStore per page.
const reapplyLocalLogPageSize = 256

// ReplayTxn is one committed transaction to apply outside 2PC: pulled from a
// peer's local log by anti-entropy, or read back from this node's own local
// log during a restore's re-apply walk (ReapplyLocalLog). Rows are exactly
// as captured - EncodedCapturedRow's Op distinguishes DML from DDL, LOAD DATA
// and vector-index control (db/meta_schema.go's OpType).
type ReplayTxn struct {
	TxnID                 uint64
	OriginNodeID          uint64 // the transaction's coordinator; owner of the DDL incarnations it creates, the same on every node that replays it
	CommitTS              hlc.Timestamp
	RequiredSchemaVersion uint64
	Rows                  []*EncodedCapturedRow
}

// ErrReplayPending is returned by ApplyReplayedTxn when this node itself
// holds t.TxnID PENDING (a prepared 2PC transaction). Nothing is applied;
// the caller retries once the local transaction resolves.
var ErrReplayPending = errors.New("replay: local transaction is PENDING")

// ErrReplayIntentConflict is returned by ApplyReplayedTxn when a row t
// touches is locally held by a different transaction's intent or CDC row
// lock. Nothing is applied; the caller retries next round. Every
// actual return uses *ReplayIntentConflictError, which also satisfies
// errors.Is(err, ErrReplayIntentConflict); match on the concrete type to read
// HolderTxnID.
var ErrReplayIntentConflict = errors.New("replay: row held by a different local transaction")

// ReplayIntentConflictError is ErrReplayIntentConflict's concrete form: it
// additionally names the local transaction (HolderTxnID) whose intent or CDC
// row lock blocked the replay - either the intent holder or the CDC row-lock
// holder - so a caller such as the log puller can act on that specific
// transaction (for example, wait for it to resolve) instead of blindly
// retrying the whole pull round.
type ReplayIntentConflictError struct {
	HolderTxnID uint64
}

// Error implements error.
func (e *ReplayIntentConflictError) Error() string {
	return fmt.Sprintf("replay: row held by local transaction %d", e.HolderTxnID)
}

// Is reports whether target is ErrReplayIntentConflict, so
// errors.Is(err, ErrReplayIntentConflict) matches every
// *ReplayIntentConflictError regardless of its HolderTxnID.
func (e *ReplayIntentConflictError) Is(target error) bool {
	return target == ErrReplayIntentConflict
}

// isDMLOp reports whether op is a row-data change (as opposed to DDL, LOAD
// DATA or vector-index control).
func isDMLOp(op OpType) bool {
	switch op {
	case OpTypeInsert, OpTypeReplace, OpTypeUpdate, OpTypeDelete:
		return true
	default:
		return false
	}
}

// markerExists reports whether __marmot_applied_txn already holds txnID, read
// through q (either the database's read pool for a preliminary check, or the
// replay's own tx for the authoritative one - see ApplyReplayedTxn).
func markerExists(q rowQuerier, txnID uint64) (bool, error) {
	var one int
	err := q.QueryRow("SELECT 1 FROM __marmot_applied_txn WHERE txn_id = ?", txnID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check applied marker: %w", err)
	}
	return true, nil
}

// LogReplayedTxn stores t's rows in mdb's local log, sealed, followed by
// StoreReplayedTransaction, unless t.TxnID is already COMMITTED there. This
// is what closes the crash window between logging a pulled txn and applying
// it: logging before the SQLite tx below means a crash between the two leaves
// the txn correctly recorded as committed-elsewhere in this node's own log,
// re-applied on the next pass because no marker exists yet. It is idempotent
// - a second call for an already-logged txn does nothing - so ApplyReplayedTxn
// can call it on every attempt. Returns the log entry's SeqNum.
//
// Held across its whole body is the txn id's stripe of
// TransactionManager.txnIDLocks, the same stripe
// BeginTransactionWithID holds around tm.metaStore.BeginTransaction. That
// makes the two atomic with each other: either this call logs t completely
// before a late local PREPARE's begin for the same id (which then finds a
// COMMITTED record and is refused - see BeginTransactionWithID's caller), or
// the begin runs first and is visible here - in which case the existing
// record is PENDING (never ABORTED: an aborted txn's immutable record is
// deleted, so GetTransaction would report it absent, not a status), and this
// call refuses with ErrReplayPending, writing nothing, exactly like
// ApplyReplayedTxn's own precheck.
func (mdb *ReplicatedDatabase) LogReplayedTxn(t *ReplayTxn) (uint64, error) {
	unlock := mdb.txnMgr.lockTxnID(t.TxnID)
	defer unlock()

	rec, err := mdb.metaStore.GetTransaction(t.TxnID)
	if err != nil {
		return 0, fmt.Errorf("check existing log entry: %w", err)
	}
	if rec != nil {
		if rec.Status == TxnStatusCommitted {
			return rec.SeqNum, nil
		}
		return 0, ErrReplayPending
	}

	for i, row := range t.Rows {
		data, err := EncodeRow(row)
		if err != nil {
			return 0, fmt.Errorf("encode captured row: %w", err)
		}
		if err := mdb.metaStore.WriteCapturedRow(t.TxnID, uint64(i+1), data); err != nil {
			return 0, fmt.Errorf("write captured row: %w", err)
		}
	}
	if err := mdb.metaStore.SealCapturedRows(t.TxnID); err != nil {
		return 0, fmt.Errorf("seal captured rows: %w", err)
	}

	origin := t.OriginNodeID
	if origin == 0 {
		origin = t.CommitTS.NodeID
	}
	if err := mdb.metaStore.StoreReplayedTransaction(t.TxnID, origin, t.CommitTS, mdb.dbName, uint32(len(t.Rows)), t.RequiredSchemaVersion); err != nil {
		return 0, fmt.Errorf("store replayed transaction: %w", err)
	}

	rec, err = mdb.metaStore.GetTransaction(t.TxnID)
	if err != nil {
		return 0, err
	}
	if rec == nil {
		return 0, fmt.Errorf("replayed transaction %d missing immediately after logging it", t.TxnID)
	}
	return rec.SeqNum, nil
}

// DiscardAbandonedBegin deletes this node's PendingBegunAbandoned record for
// txnID (MetaStore.DiscardAbandonedBegin), holding the txn id's stripe of
// TransactionManager.txnIDLocks so a PREPARE beginning the same id cannot
// interleave with the discard.
func (mdb *ReplicatedDatabase) DiscardAbandonedBegin(txnID uint64) error {
	unlock := mdb.txnMgr.lockTxnID(txnID)
	defer unlock()
	return mdb.metaStore.DiscardAbandonedBegin(txnID)
}

// ApplyReplayedTxn applies a committed transaction pulled from a peer's log
// (or, with logFirst=false, read back from this node's own local log by
// ReapplyLocalLog), exactly-once and atomically with the applied-txn
// marker.
//
// Order:
//  1. If this node itself holds t.TxnID PENDING, return ErrReplayPending and
//     touch nothing: the local transaction must resolve first.
//  2. For every DML row, refuse (ErrReplayIntentConflict, touching nothing)
//     if the row's key holds a local intent or CDC row lock owned by a
//     different transaction. This stops replay from writing over a newer
//     image a locally in-flight (or about-to-be-applied) transaction holds.
//  3. If logFirst, log t first (LogReplayedTxn) - see its doc comment for the
//     crash-window this closes.
//  4. Vector-control rows: gated on the marker (checked read-only, before the
//     SQLite tx, since the control is not SQLite and cannot share it) so a
//     replay that already committed never re-runs the control. See
//     TransactionManager.applyNonDMLIntents's doc comment for why re-running
//     it once, on a crash between this step and the marker tx below, is safe.
//  5. One SQLite tx: re-check the marker inside the tx (authoritative - the
//     read-only check above can race a concurrent apply); if present, roll
//     back and return applied=false. Otherwise apply every remaining row (DML
//     via ApplyCDCValues, DDL via ApplyReplayedDDL with owner=t.OriginNodeID,
//     LOAD DATA via ApplyLoadDataInTx), finish any schema change, bump the
//     schema version iff the txn carries DDL, write the marker, commit.
//  6. After commit: reload the schema cache and refresh the read pool if DDL
//     changed it (warn-only, matching handleReplay's existing contract),
//     update the cached schema version, then apply committed vector CDC for
//     the DML rows (idempotent by txn/seq; errors are logged, not returned -
//     the row data itself already committed).
//
// A claim-only txn (zero rows - an AUTO_INCREMENT range claim, which never
// travels in CDC) reaches step 5 with nothing to apply but the marker itself:
// see applyNonDMLIntents's doc comment for why that is safe though not
// exactly-once.
func (mdb *ReplicatedDatabase) ApplyReplayedTxn(ctx context.Context, t *ReplayTxn, logFirst bool) (applied bool, err error) {
	rec, err := mdb.replayPrecheck(t)
	if err != nil {
		return false, err
	}

	var seqNum uint64
	if logFirst {
		seqNum, err = mdb.LogReplayedTxn(t)
		if err != nil {
			return false, fmt.Errorf("log replayed transaction: %w", err)
		}
	} else if rec != nil {
		seqNum = rec.SeqNum
	}

	if err := mdb.applyReplayVectorControl(ctx, t); err != nil {
		return false, err
	}

	result, applied, err := mdb.commitReplayedTx(ctx, t, seqNum)
	if err != nil || !applied {
		return false, err
	}

	mdb.afterReplayCommit(ctx, t, seqNum, result.change, result.hasDDL, result.newSchemaVersion, result.dmlEntries)
	return true, nil
}

// replayTxCommitResult bundles what commitReplayedTx's SQLite tx produced,
// for its caller's post-commit refresh (afterReplayCommit).
type replayTxCommitResult struct {
	change           *SchemaChange
	dmlEntries       []common.CDCEntry
	hasDDL           bool
	newSchemaVersion uint64
}

// commitReplayedTx is ApplyReplayedTxn's step 5: one SQLite tx that
// re-checks the marker (authoritative - the read-only check in
// applyReplayVectorControl can race a concurrent apply), applies every
// remaining row, finishes any schema change, bumps the schema version iff
// the txn carries DDL, writes the marker, and commits. applied is false,
// with no error, when the marker already existed - nothing else is touched.
func (mdb *ReplicatedDatabase) commitReplayedTx(ctx context.Context, t *ReplayTxn, seqNum uint64) (*replayTxCommitResult, bool, error) {
	tx, err := mdb.writeDB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("begin replay transaction: %w", err)
	}
	defer tx.Rollback()

	if already, err := markerExists(tx, t.TxnID); err != nil {
		return nil, false, err
	} else if already {
		return nil, false, nil
	}

	schemaAdapter := &schemaCacheAdapter{cache: mdb.schemaCache}
	var change SchemaChange
	dmlEntries, hasDDL, err := mdb.applyReplayedRowsInTx(ctx, tx, t, seqNum, schemaAdapter, &change)
	if err != nil {
		return nil, false, err
	}

	if !change.Empty() {
		if err := mdb.FinishReplayedSchemaChange(tx, &change); err != nil {
			return nil, false, fmt.Errorf("finish replayed schema change: %w", err)
		}
	}

	var newSchemaVersion uint64
	if hasDDL {
		newSchemaVersion, err = bumpSchemaVersionInTx(tx)
		if err != nil {
			return nil, false, fmt.Errorf("bump schema version: %w", err)
		}
	}

	if err := MarkSQLiteTxnApplied(tx, t.TxnID, t.CommitTS); err != nil {
		return nil, false, err
	}

	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("commit replay transaction: %w", err)
	}

	return &replayTxCommitResult{change: &change, dmlEntries: dmlEntries, hasDDL: hasDDL, newSchemaVersion: newSchemaVersion}, true, nil
}

// replayPrecheck is ApplyReplayedTxn's step 1-2 precheck: refuse to touch
// anything if this node itself holds t.TxnID PENDING (ErrReplayPending), or
// if a DML row t touches is locally held, by intent or CDC row lock, by a
// different transaction (*ReplayIntentConflictError, matching
// errors.Is(err, ErrReplayIntentConflict)). Returns t's existing log
// record, if any, for the caller to reuse its SeqNum.
func (mdb *ReplicatedDatabase) replayPrecheck(t *ReplayTxn) (*TransactionRecord, error) {
	rec, err := mdb.metaStore.GetTransaction(t.TxnID)
	if err != nil {
		return nil, fmt.Errorf("check local transaction status: %w", err)
	}
	if rec != nil && rec.Status == TxnStatusPending {
		return nil, ErrReplayPending
	}

	for _, row := range t.Rows {
		if !isDMLOp(OpType(row.Op)) || len(row.IntentKey) == 0 {
			continue
		}
		key := string(row.IntentKey)
		if intent, ierr := mdb.metaStore.GetIntent(row.Table, key); ierr == nil && intent != nil && intent.TxnID != t.TxnID {
			return nil, &ReplayIntentConflictError{HolderTxnID: intent.TxnID}
		}
		if lockTxnID, lerr := mdb.metaStore.GetCDCRowLock(row.Table, key); lerr == nil && lockTxnID != 0 && lockTxnID != t.TxnID {
			return nil, &ReplayIntentConflictError{HolderTxnID: lockTxnID}
		}
	}
	return rec, nil
}

// applyReplayVectorControl runs t's vector-index control rows, if any,
// outside SQLite (it is not SQLite and cannot share the tx ApplyReplayedTxn
// opens afterward), gated read-only on the applied-txn marker so a replay
// that already committed never re-runs the control. See
// TransactionManager.applyNonDMLIntents's doc comment for why re-running it
// once, on a crash between this step and the marker tx, is safe.
func (mdb *ReplicatedDatabase) applyReplayVectorControl(ctx context.Context, t *ReplayTxn) error {
	already, err := markerExists(mdb.readDB, t.TxnID)
	if err != nil {
		return err
	}
	if already {
		return nil
	}
	notifier := mdb.txnMgr.VectorCDCNotifier()
	for _, row := range t.Rows {
		if OpType(row.Op) != OpTypeVectorIndex || row.VectorIndexChange == nil {
			continue
		}
		controlApplier, ok := notifier.(interface {
			ApplyVectorControl(context.Context, common.VectorIndexChange) error
		})
		if !ok {
			return fmt.Errorf("vector index control %s: vector manager not configured", row.VectorIndexChange.IndexName)
		}
		if err := controlApplier.ApplyVectorControl(ctx, *row.VectorIndexChange); err != nil {
			return fmt.Errorf("apply vector index control: %w", err)
		}
	}
	return nil
}

// applyReplayedRowsInTx applies every row of t inside tx - DML via
// ApplyCDCValues, DDL via ApplyReplayedDDL (accumulating into change), LOAD
// DATA via ApplyLoadDataInTx - and returns the CDC entries to notify vector
// CDC with after commit, and whether any row was DDL.
func (mdb *ReplicatedDatabase) applyReplayedRowsInTx(ctx context.Context, tx *sql.Tx, t *ReplayTxn, seqNum uint64, schemaAdapter *schemaCacheAdapter, change *SchemaChange) ([]common.CDCEntry, bool, error) {
	hasDDL := false
	dmlEntries := make([]common.CDCEntry, 0, len(t.Rows))

	for _, row := range t.Rows {
		switch OpType(row.Op) {
		case OpTypeInsert, OpTypeReplace, OpTypeUpdate, OpTypeDelete:
			if err := ApplyCDCValues(tx, schemaAdapter, OpType(row.Op), row.Table, row.OldValues, row.NewValues); err != nil {
				return nil, false, fmt.Errorf("apply replayed CDC row for %s: %w", row.Table, err)
			}
			dmlEntries = append(dmlEntries, common.CDCEntry{
				Table:        row.Table,
				IntentKey:    row.IntentKey,
				Operation:    row.Op,
				OldValues:    row.OldValues,
				NewValues:    row.NewValues,
				CommitTxnID:  t.TxnID,
				CommitSeqNum: seqNum,
			})
		case OpTypeDDL:
			if err := mdb.ApplyReplayedDDL(ctx, tx, row.DDLSQL, row.Table, t.OriginNodeID, change); err != nil {
				return nil, false, fmt.Errorf("apply replayed DDL: %w", err)
			}
			hasDDL = true
		case OpTypeLoadData:
			if _, err := ApplyLoadDataInTx(tx, row.LoadSQL, row.LoadData); err != nil {
				return nil, false, fmt.Errorf("apply replayed LOAD DATA: %w", err)
			}
		case OpTypeVectorIndex:
			// Applied outside this tx above (or already applied on an earlier
			// attempt, per the marker check); nothing left to do here.
		default:
			return nil, false, fmt.Errorf("unsupported replayed row op %d for table %s", row.Op, row.Table)
		}
	}

	return dmlEntries, hasDDL, nil
}

// afterReplayCommit runs ApplyReplayedTxn's step 6 post-commit refresh:
// reload the schema cache and refresh the read pool when DDL changed it,
// advance the cached schema version, and apply committed vector CDC for the
// DML rows. Every failure here is logged, not returned - the row data itself
// already committed in SQLite.
func (mdb *ReplicatedDatabase) afterReplayCommit(ctx context.Context, t *ReplayTxn, seqNum uint64, change *SchemaChange, hasDDL bool, newSchemaVersion uint64, dmlEntries []common.CDCEntry) {
	if !change.Empty() {
		if err := mdb.ReloadSchema(); err != nil {
			log.Warn().Err(err).Uint64("txn_id", t.TxnID).Str("database", mdb.dbName).Msg("ApplyReplayedTxn: failed to reload schema after DDL")
		}
	}
	if hasDDL {
		mdb.advanceSchemaVersion(newSchemaVersion)
		mdb.RefreshReadPool()
	}

	if len(dmlEntries) > 0 {
		if notifier := mdb.txnMgr.VectorCDCNotifier(); notifier != nil {
			if err := notifier.ApplyCommittedVectorCDC(ctx, mdb.dbName, t.TxnID, seqNum, dmlEntries); err != nil {
				log.Error().Err(err).Uint64("txn_id", t.TxnID).Str("database", mdb.dbName).
					Msg("ApplyReplayedTxn: vector CDC failed after row commit; local vector index is dirty")
			}
		}
	}
}

// AppliedTxns returns, for every id in ids, whether __marmot_applied_txn
// holds a marker for it. Looked up through the read pool in chunks to bound
// the IN clause's size.
func (mdb *ReplicatedDatabase) AppliedTxns(ids []uint64) (map[uint64]bool, error) {
	result := make(map[uint64]bool, len(ids))
	const chunkSize = 512

	for start := 0; start < len(ids); start += chunkSize {
		end := start + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[start:end]

		placeholders := make([]byte, 0, len(chunk)*2)
		args := make([]interface{}, len(chunk))
		for i, id := range chunk {
			if i > 0 {
				placeholders = append(placeholders, ',')
			}
			placeholders = append(placeholders, '?')
			args[i] = id
			result[id] = false
		}

		query := fmt.Sprintf("SELECT txn_id FROM __marmot_applied_txn WHERE txn_id IN (%s)", string(placeholders))
		rows, err := mdb.readDB.Query(query, args...)
		if err != nil {
			return nil, fmt.Errorf("query applied markers: %w", err)
		}
		for rows.Next() {
			var id uint64
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, fmt.Errorf("scan applied marker: %w", err)
			}
			result[id] = true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}

	return result, nil
}

// ReapplyLocalLog walks this database's own local log, in position order,
// and re-applies (through ApplyReplayedTxn with logFirst=false, since the
// entry is already in this node's own log) every COMMITTED entry whose
// marker a restored SQLite file lacks. Used after a snapshot restore: the
// restored file may lack markers for transactions this node's own log
// already holds durably, that the snapshot's source itself lacked.
//
// An entry that cannot be applied - PENDING, an intent conflict, a
// capture/decode error, or a captured row count that does not match the log
// record's own RowCount (never applying a partial transaction) - does
// not stop the walk: every other entry is still re-applied, and the first
// such error is returned with the count of entries newly applied, so the
// caller keeps the re-apply pending and runs it again. A repeated run is
// always safe, since ApplyReplayedTxn applies nothing whose marker exists.
func (mdb *ReplicatedDatabase) ReapplyLocalLog(ctx context.Context) (int, error) {
	applied := 0
	var firstErr error
	after := LogPosition{}

	for {
		if err := ctx.Err(); err != nil {
			return applied, err
		}
		entries, _, more, err := mdb.metaStore.ListCommittedLog(after, reapplyLocalLogPageSize)
		if err != nil {
			return applied, fmt.Errorf("list local log: %w", err)
		}

		ids := make([]uint64, len(entries))
		for i, pos := range entries {
			ids[i] = pos.TxnID
		}
		markers, err := mdb.AppliedTxns(ids)
		if err != nil {
			return applied, err
		}

		for _, pos := range entries {
			after = pos
			if markers[pos.TxnID] {
				continue
			}
			replayed, err := mdb.reapplyLocalEntry(ctx, pos.TxnID)
			if err != nil {
				if firstErr == nil {
					firstErr = fmt.Errorf("reapply txn %d: %w", pos.TxnID, err)
				}
				continue
			}
			if replayed {
				applied++
			}
		}

		if !more {
			return applied, firstErr
		}
	}
}

// ReapplyLocalLogIfPending runs ReapplyLocalLog when the durable
// re-apply-pending mark (MetaStore.SetReapplyPending) is set, and clears the
// mark only once a run succeeds. It is a no-op otherwise.
func (mdb *ReplicatedDatabase) ReapplyLocalLogIfPending(ctx context.Context) (int, error) {
	pending, err := mdb.metaStore.ReapplyPending()
	if err != nil || !pending {
		return 0, err
	}
	applied, err := mdb.ReapplyLocalLog(ctx)
	if err != nil {
		return applied, err
	}
	return applied, mdb.metaStore.SetReapplyPending(false)
}

// reapplyLocalEntry re-applies one committed entry of this node's own log
// from its locally captured rows.
func (mdb *ReplicatedDatabase) reapplyLocalEntry(ctx context.Context, txnID uint64) (bool, error) {
	rec, err := mdb.metaStore.GetTransaction(txnID)
	if err != nil {
		return false, fmt.Errorf("read local log entry: %w", err)
	}
	if rec == nil || rec.Status != TxnStatusCommitted {
		return false, nil
	}
	rows, err := decodeLocalCapturedRows(mdb.metaStore, txnID, rec.RowCount)
	if err != nil {
		return false, fmt.Errorf("read captured rows: %w", err)
	}
	return mdb.ApplyReplayedTxn(ctx, &ReplayTxn{
		TxnID:                 txnID,
		OriginNodeID:          rec.NodeID,
		CommitTS:              hlc.Timestamp{WallTime: rec.CommitTSWall, Logical: rec.CommitTSLogical, NodeID: rec.NodeID},
		RequiredSchemaVersion: rec.RequiredSchemaVersion,
		Rows:                  rows,
	}, false)
}

// decodeLocalCapturedRows reads and decodes every captured row this node's
// own meta store holds for txnID, and refuses a partial result: a capture or
// decode error, or a row count that does not match expected (the log
// record's own RowCount), fails outright rather than silently replaying a
// truncated transaction.
func decodeLocalCapturedRows(metaStore MetaStore, txnID uint64, expected uint32) ([]*EncodedCapturedRow, error) {
	cursor, err := metaStore.IterateCapturedRows(txnID)
	if err != nil {
		return nil, fmt.Errorf("iterate captured rows: %w", err)
	}
	defer cursor.Close()

	rows := make([]*EncodedCapturedRow, 0, expected)
	for cursor.Next() {
		_, data := cursor.Row()
		row, err := DecodeRow(data)
		if err != nil {
			return nil, fmt.Errorf("decode captured row: %w", err)
		}
		rows = append(rows, row)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate captured rows: %w", err)
	}

	if uint32(len(rows)) != expected {
		return nil, fmt.Errorf("captured row count mismatch: got %d want %d", len(rows), expected)
	}
	return rows, nil
}
