package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
)

type exactVectorFetcher struct {
	stmt *sql.Stmt
	spec vecindex.IVFSpec
}

func newExactVectorFetcher(ctx context.Context, db *sql.DB, meta common.VectorIndexMeta, spec vecindex.IVFSpec) (*exactVectorFetcher, error) {
	if db == nil {
		return nil, nil
	}
	query := fmt.Sprintf(
		"SELECT %s FROM %s WHERE rowid=?",
		quoteIdent(meta.ColumnName),
		quoteIdent(meta.TableName),
	)
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &exactVectorFetcher{stmt: stmt, spec: spec}, nil
}

func (f *exactVectorFetcher) Close() {
	if f == nil || f.stmt == nil {
		return
	}
	_ = f.stmt.Close()
}

func (f *exactVectorFetcher) Prepared(ctx context.Context, rowID int64) ([]byte, bool, error) {
	if f == nil || f.stmt == nil || rowID == 0 {
		return nil, false, nil
	}
	rows, err := f.stmt.QueryContext(ctx, rowID)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	var raw sql.RawBytes
	if err := rows.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if len(raw) == 0 {
		return nil, false, nil
	}
	prepared, err := materializeVectorBlob(raw, f.spec.Metric, f.spec.Dim, f.spec.MaxNorm)
	if err != nil {
		return nil, false, err
	}
	if prepared == nil {
		return nil, false, nil
	}
	return prepared, true, nil
}
