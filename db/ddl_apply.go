package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
)

// ApplyDDLSQLInTx rewrites DDL for idempotent replay and executes it in the
// provided transaction. It is for a read-only replica, which issues no
// AUTO_INCREMENT ids; a cluster node replays DDL through
// ReplicatedDatabase.ApplyReplayedDDL, which also records the change to its
// tables (SchemaChange).
func ApplyDDLSQLInTx(ctx context.Context, tx *sql.Tx, ddlSQL string) error {
	if tx == nil {
		return fmt.Errorf("transaction is nil")
	}
	idempotentSQL := protocol.RewriteDDLForIdempotency(ddlSQL)
	if _, err := tx.ExecContext(ctx, idempotentSQL); err != nil {
		return err
	}
	return nil
}
