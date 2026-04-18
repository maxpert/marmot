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
	dbMgr interface {
		GetDatabaseConnection(name string) (*sql.DB, error)
	}
}

// NewSQLDeltaFlushDB creates a DeltaFlushDB backed by the given database.
func NewSQLDeltaFlushDB(dbMgr interface {
	GetDatabaseConnection(name string) (*sql.DB, error)
}) *SQLDeltaFlushDB {
	return &SQLDeltaFlushDB{dbMgr: dbMgr}
}

// FetchDeltaEmbeddings reads internal vector blobs directly from the sidecar
// delta partition (cluster_id=0).
func (s *SQLDeltaFlushDB) FetchDeltaEmbeddings(
	ctx context.Context,
	database, indexName, tableName, columnName string,
	limit int,
) ([]vecindex.DeltaRow, error) {
	db, err := s.dbMgr.GetDatabaseConnection(database)
	if err != nil {
		return nil, fmt.Errorf("delta flush get db %s: %w", database, err)
	}
	membersQ := quoteIdent(vecindex.MembersTable(indexName))

	rows, err := db.QueryContext(ctx,
		fmt.Sprintf(
			`SELECT rowid, vec FROM %s WHERE cluster_id = 0 LIMIT ?`,
			membersQ),
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

// CommitFlushBatch atomically performs an exact-token delete from cluster_id=0
// and reinserts the row into its assigned cluster only if the delete matched.
func (s *SQLDeltaFlushDB) CommitFlushBatch(
	ctx context.Context,
	database, indexName string,
	assignments []vecindex.DeltaAssignment,
) error {
	if len(assignments) == 0 {
		return nil
	}
	db, err := s.dbMgr.GetDatabaseConnection(database)
	if err != nil {
		return fmt.Errorf("delta flush get db %s: %w", database, err)
	}
	membersQ := quoteIdent(vecindex.MembersTable(indexName))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delta flush begin: %w", err)
	}
	defer func() {
		if tx != nil {
			_ = tx.Rollback()
		}
	}()

	delStmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE rowid = ? AND cluster_id = 0 AND vec = ?`, membersQ))
	if err != nil {
		return fmt.Errorf("delta flush prepare delete: %w", err)
	}
	defer delStmt.Close()

	insStmt, err := tx.PrepareContext(ctx,
		fmt.Sprintf(`INSERT OR REPLACE INTO %s (cluster_id, rowid, vec) VALUES (?, ?, ?)`, membersQ))
	if err != nil {
		return fmt.Errorf("delta flush prepare insert: %w", err)
	}
	defer insStmt.Close()

	for _, a := range assignments {
		res, err := delStmt.ExecContext(ctx, a.Rowid, a.Embed)
		if err != nil {
			return fmt.Errorf("delta flush delete rowid %d: %w", a.Rowid, err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("delta flush rows-affected rowid %d: %w", a.Rowid, err)
		}
		if n == 0 {
			continue
		}
		if _, err := insStmt.ExecContext(ctx, a.ClusterID, a.Rowid, a.Embed); err != nil {
			return fmt.Errorf("delta flush insert rowid %d: %w", a.Rowid, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("delta flush commit: %w", err)
	}
	tx = nil
	return nil
}
