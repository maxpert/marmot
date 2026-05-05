package db

import (
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/hlc"
)

const appliedTxnTableDDL = `
CREATE TABLE IF NOT EXISTS __marmot_applied_txn (
	txn_id INTEGER PRIMARY KEY,
	commit_node INTEGER NOT NULL,
	commit_wall INTEGER NOT NULL,
	commit_logical INTEGER NOT NULL,
	applied_at INTEGER NOT NULL DEFAULT (unixepoch('now') * 1000000000)
)`

func ensureAppliedTxnTable(db *sql.DB) error {
	if db == nil {
		return nil
	}
	if _, err := db.Exec(appliedTxnTableDDL); err != nil {
		return fmt.Errorf("create applied txn marker table: %w", err)
	}
	return nil
}

func MarkSQLiteTxnApplied(tx *sql.Tx, txnID uint64, commitTS hlc.Timestamp) error {
	if tx == nil {
		return nil
	}
	_, err := tx.Exec(
		"INSERT OR IGNORE INTO __marmot_applied_txn (txn_id, commit_node, commit_wall, commit_logical) VALUES (?, ?, ?, ?)",
		txnID,
		commitTS.NodeID,
		commitTS.WallTime,
		commitTS.Logical,
	)
	if err != nil {
		return fmt.Errorf("mark sqlite txn applied: %w", err)
	}
	return nil
}

func repairAppliedTxnMetadata(db *sql.DB, metaStore MetaStore, dbName string) error {
	if db == nil || metaStore == nil {
		return nil
	}
	rows, err := db.Query("SELECT txn_id, commit_node, commit_wall, commit_logical FROM __marmot_applied_txn")
	if err != nil {
		return fmt.Errorf("read applied txn markers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var txnIDRaw, nodeIDRaw int64
		var wall int64
		var logical int32
		if err := rows.Scan(&txnIDRaw, &nodeIDRaw, &wall, &logical); err != nil {
			return err
		}
		txnID := uint64(txnIDRaw)
		nodeID := uint64(nodeIDRaw)
		rec, err := metaStore.GetTransaction(txnID)
		if err != nil {
			return err
		}
		if rec != nil && rec.Status == TxnStatusCommitted {
			continue
		}
		entries, err := metaStore.GetIntentEntries(txnID)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			continue
		}
		commitTS := hlc.Timestamp{WallTime: wall, Logical: logical, NodeID: nodeID}
		if rec != nil {
			if err := metaStore.CommitTransaction(txnID, commitTS, nil, dbName, "", 0, uint32(len(entries))); err != nil {
				return err
			}
			continue
		}
		if err := metaStore.StoreReplayedTransaction(txnID, nodeID, commitTS, dbName, uint32(len(entries))); err != nil {
			return err
		}
	}
	return rows.Err()
}
