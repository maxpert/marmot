package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
)

func packedStorePath(dbPath, indexName string) string {
	return dbPath + "." + indexName + ".vecpack"
}

// EnsurePackedPartitionStore loads the packed stable-partition snapshot for an
// index, rebuilding it from the members table when absent or invalid.
func EnsurePackedPartitionStore(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) (*vecindex.PackedPartitionStore, error) {
	path := packedStorePath(dbPath, meta.IndexName)
	store, err := vecindex.OpenPackedPartitionStore(path)
	if err == nil {
		if store.Dim() == spec.InternalDim() {
			return store, nil
		}
		_ = store.Close()
	}
	if err != nil && !os.IsNotExist(err) {
		_ = os.Remove(path)
	}
	return RebuildPackedPartitionStore(ctx, db, dbPath, meta, spec)
}

// RebuildPackedPartitionStore rebuilds the packed stable-partition snapshot
// from the members table and returns a freshly mmap'd reader.
func RebuildPackedPartitionStore(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) (*vecindex.PackedPartitionStore, error) {
	if db == nil {
		return nil, fmt.Errorf("packed store rebuild: db is nil")
	}
	var maxCluster int
	query := fmt.Sprintf(`SELECT COALESCE(MAX(cluster_id), 0) FROM %s WHERE cluster_id > 0`,
		quoteIdent(vecindex.MembersTable(meta.IndexName)))
	if err := db.QueryRowContext(ctx, query).Scan(&maxCluster); err != nil {
		return nil, fmt.Errorf("packed store max cluster: %w", err)
	}
	if maxCluster == 0 {
		return nil, nil
	}

	writer, err := vecindex.CreatePackedPartitionStoreWriter(
		packedStorePath(dbPath, meta.IndexName),
		spec.InternalDim(),
		maxCluster,
	)
	if err != nil {
		return nil, err
	}
	defer writer.Abort()

	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		`SELECT cluster_id, rowid, vec FROM %s WHERE cluster_id > 0 ORDER BY cluster_id, rowid`,
		quoteIdent(vecindex.MembersTable(meta.IndexName)),
	))
	if err != nil {
		return nil, fmt.Errorf("packed store rows: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var clusterID, rowid int64
		var vec []byte
		if err := rows.Scan(&clusterID, &rowid, &vec); err != nil {
			return nil, fmt.Errorf("packed store scan: %w", err)
		}
		if err := writer.Append(clusterID, rowid, vec); err != nil {
			return nil, err
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("packed store iter: %w", err)
	}
	store, err := writer.Close()
	if err != nil {
		return nil, err
	}
	return store, nil
}

func buildAndStorePackedPartitionStore(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	state *vecindex.IndexState,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) error {
	if state == nil || dbPath == "" {
		return nil
	}
	store, err := EnsurePackedPartitionStore(ctx, db, dbPath, meta, spec)
	if err != nil {
		return err
	}
	state.StorePackedStore(store)
	return nil
}

// BuildPackedPartitionStoreOnReopen restores the packed stable-partition
// snapshot for an already-registered IndexState.
func BuildPackedPartitionStoreOnReopen(
	ctx context.Context,
	db *sql.DB,
	dbPath string,
	state *vecindex.IndexState,
	meta common.VectorIndexMeta,
	spec vecindex.IVFSpec,
) error {
	return buildAndStorePackedPartitionStore(ctx, db, dbPath, state, meta, spec)
}
