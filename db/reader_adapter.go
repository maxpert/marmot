package db

import (
	"context"

	"github.com/maxpert/marmot/coordinator"
)

// LocalReader implements coordinator.Reader for local database access
type LocalReader struct {
	dbMgr DatabaseProvider
}

// NewLocalReader creates a new local reader
func NewLocalReader(dbMgr DatabaseProvider) *LocalReader {
	return &LocalReader{
		dbMgr: dbMgr,
	}
}

// ReadSnapshot executes a read query on the local database with snapshot isolation
func (r *LocalReader) ReadSnapshot(ctx context.Context, nodeID uint64, req *coordinator.ReadRequest) (*coordinator.ReadResponse, error) {
	// Get database
	replicatedDB, err := r.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &coordinator.ReadResponse{
			Success: false,
			Error:   "database not found: " + req.Database,
		}, nil
	}

	// Execute snapshot read
	columns, rows, err := replicatedDB.ExecuteSnapshotRead(ctx, req.Query, req.Args...)
	if err != nil {
		return &coordinator.ReadResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &coordinator.ReadResponse{
		Success:  true,
		Rows:     rows,
		Columns:  columns,
		RowCount: len(rows),
	}, nil
}
