package db

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/maxpert/marmot/modules/vecindex"
)

// Compile-time check that SQLDeltaFlushDB implements the interface.
var _ vecindex.DeltaFlushDB = (*SQLDeltaFlushDB)(nil)

// SQLDeltaFlushDB implements vecindex.DeltaFlushDB against a SQLite database.
type SQLDeltaFlushDB struct {
	db *sql.DB
}

// NewSQLDeltaFlushDB creates a DeltaFlushDB backed by the given database.
func NewSQLDeltaFlushDB(db *sql.DB) *SQLDeltaFlushDB {
	return &SQLDeltaFlushDB{db: db}
}

// FetchDeltaEmbeddings joins the members table (cluster_id=0) with the base
// table to return up to limit (rowid, embedding) pairs for assignment.
func (s *SQLDeltaFlushDB) FetchDeltaEmbeddings(
	ctx context.Context,
	indexName, tableName, columnName string,
	limit int,
) ([]vecindex.DeltaRow, error) {
	membersQ := quoteIdent(vecindex.MembersTable(indexName))
	tableQ := quoteIdent(tableName)
	colQ := quoteIdent(columnName)

	rows, err := s.db.QueryContext(ctx,
		fmt.Sprintf(
			`SELECT m.rowid, b.%s FROM %s m JOIN %s b ON b.rowid = m.rowid
			 WHERE m.cluster_id = 0 AND b.%s IS NOT NULL LIMIT ?`,
			colQ, membersQ, tableQ, colQ),
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("delta flush fetch: %w", err)
	}
	defer rows.Close()

	var result []vecindex.DeltaRow
	for rows.Next() {
		var dr vecindex.DeltaRow
		if err := rows.Scan(&dr.Rowid, &dr.Embed); err != nil {
			return nil, fmt.Errorf("delta flush scan: %w", err)
		}
		result = append(result, dr)
	}
	return result, rows.Err()
}

// CommitFlushBatch atomically deletes the given rowids from cluster_id=0 and
// inserts them with their assigned cluster_id within a single transaction.
func (s *SQLDeltaFlushDB) CommitFlushBatch(
	ctx context.Context,
	indexName string,
	assignments []vecindex.DeltaAssignment,
) error {
	if len(assignments) == 0 {
		return nil
	}
	membersQ := quoteIdent(vecindex.MembersTable(indexName))

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delta flush begin: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	delStmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE rowid = ? AND cluster_id = 0`, membersQ))
	if err != nil {
		return fmt.Errorf("delta flush prepare delete: %w", err)
	}
	defer delStmt.Close()

	insStmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf(`INSERT OR IGNORE INTO %s (cluster_id, rowid) VALUES (?, ?)`, membersQ))
	if err != nil {
		return fmt.Errorf("delta flush prepare insert: %w", err)
	}
	defer insStmt.Close()

	for _, a := range assignments {
		if _, err := delStmt.ExecContext(ctx, a.Rowid); err != nil {
			return fmt.Errorf("delta flush delete rowid %d: %w", a.Rowid, err)
		}
		if _, err := insStmt.ExecContext(ctx, a.ClusterID, a.Rowid); err != nil {
			return fmt.Errorf("delta flush insert rowid %d: %w", a.Rowid, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delta flush commit: %w", err)
	}
	tx = nil
	return nil
}
