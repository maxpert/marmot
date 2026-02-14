package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/protocol"
)

// ApplyDDLSQLInTx rewrites DDL for idempotent replay and executes it in the
// provided transaction.
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

// ApplyDDLSQL rewrites DDL for idempotent replay and executes it directly
// against a DB connection.
func ApplyDDLSQL(dbConn *sql.DB, ddlSQL string) error {
	if dbConn == nil {
		return fmt.Errorf("database handle is nil")
	}
	idempotentSQL := protocol.RewriteDDLForIdempotency(ddlSQL)
	if _, err := dbConn.ExecContext(context.Background(), idempotentSQL); err != nil {
		return err
	}
	return nil
}
