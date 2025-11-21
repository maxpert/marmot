package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ReadCoordinator orchestrates distributed reads with MVCC snapshot isolation
// Implements snapshot reads: read consistent data as of transaction start time
// Uses full database replication model: ALL nodes have complete data, so reads
// can be served from ANY node. Consistency level determines read quorum.
type ReadCoordinator struct {
	nodeID       uint64
	nodeProvider NodeProvider
	reader       Reader
	timeout      time.Duration
}

// Reader executes read queries on local or remote nodes
type Reader interface {
	ReadSnapshot(ctx context.Context, nodeID uint64, req *ReadRequest) (*ReadResponse, error)
}

// ReadRequest for snapshot reads
type ReadRequest struct {
	// SQL query to execute
	Query string
	Args  []interface{}

	// Snapshot timestamp - read data as of this time
	SnapshotTS hlc.Timestamp

	// Consistency level
	Consistency protocol.ConsistencyLevel

	// Table being read (for replica routing)
	TableName string

	// Target database name
	Database string
}

// ReadResponse from a node
type ReadResponse struct {
	Success bool
	Error   string
	// Rows returned (serialized)
	Rows []map[string]interface{}
	// Columns preserves the order of columns
	Columns []string
	// Number of rows
	RowCount int
}

// NewReadCoordinator creates a new read coordinator for full database replication
func NewReadCoordinator(nodeID uint64, nodeProvider NodeProvider, reader Reader,
	timeout time.Duration) *ReadCoordinator {

	return &ReadCoordinator{
		nodeID:       nodeID,
		nodeProvider: nodeProvider,
		reader:       reader,
		timeout:      timeout,
	}
}

// ReadTransaction executes a read query with MVCC snapshot isolation
// This is the CRITICAL path: Snapshot reads with quorum validation
//
// Full Replication Read Model:
// - ALL nodes have complete data, so reads can be served from ANY node
// - LOCAL_ONE: Read from self (coordinator) only - fastest
// - ONE: Read from any single alive node
// - QUORUM: Read from majority of nodes to ensure consistency
// - ALL: Read from all alive nodes
func (rc *ReadCoordinator) ReadTransaction(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	// Get all alive nodes from cluster
	allNodes, err := rc.nodeProvider.GetAliveNodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get alive nodes: %w", err)
	}

	clusterSize := len(allNodes)
	if clusterSize == 0 {
		return nil, fmt.Errorf("no alive nodes in cluster")
	}

	// Validate consistency level against cluster size
	if err := ValidateConsistencyLevel(req.Consistency, clusterSize); err != nil {
		return nil, fmt.Errorf("invalid read consistency: %w", err)
	}

	// Calculate required quorum
	requiredQuorum := QuorumSize(req.Consistency, clusterSize)

	// For LOCAL_ONE, read from self (coordinator node)
	if req.Consistency == protocol.ConsistencyLocalOne {
		resp, err := rc.reader.ReadSnapshot(ctx, rc.nodeID, req)
		if err != nil {
			return nil, fmt.Errorf("local read failed: %w", err)
		}
		if resp.Success {
			return resp, nil
		}
		return nil, fmt.Errorf("local read returned error: %s", resp.Error)
	}

	// For ONE, read from first available node (try self first for locality)
	if req.Consistency == protocol.ConsistencyOne {
		// Try self first
		resp, err := rc.reader.ReadSnapshot(ctx, rc.nodeID, req)
		if err == nil && resp.Success {
			return resp, nil
		}

		// Try other nodes
		for _, nodeID := range allNodes {
			if nodeID == rc.nodeID {
				continue // Already tried self
			}
			resp, err := rc.reader.ReadSnapshot(ctx, nodeID, req)
			if err != nil {
				continue // Try next node
			}
			if resp.Success {
				return resp, nil
			}
		}
		return nil, fmt.Errorf("all nodes failed for CONSISTENCY_ONE read")
	}

	// For QUORUM/ALL, read from multiple nodes
	responses, err := rc.readFromNodes(ctx, allNodes, req)
	if err != nil {
		return nil, fmt.Errorf("read failed: %w", err)
	}

	// Check if quorum was achieved
	if len(responses) < requiredQuorum {
		return nil, fmt.Errorf("read quorum not achieved: got %d responses, need %d out of %d nodes",
			len(responses), requiredQuorum, clusterSize)
	}

	// Return first successful response
	// In a production system, we'd verify consistency across replicas (read repair)
	for _, resp := range responses {
		if resp.Success {
			return resp, nil
		}
	}

	return nil, fmt.Errorf("no successful reads from quorum")
}

// readFromNodes executes read on multiple nodes
func (rc *ReadCoordinator) readFromNodes(ctx context.Context, nodeIDs []uint64,
	req *ReadRequest) (map[uint64]*ReadResponse, error) {

	ctx, cancel := context.WithTimeout(ctx, rc.timeout)
	defer cancel()

	responses := make(map[uint64]*ReadResponse)
	responseChan := make(chan struct {
		nodeID uint64
		resp   *ReadResponse
	}, len(nodeIDs))

	for _, nodeID := range nodeIDs {
		go func(nid uint64) {
			resp, err := rc.reader.ReadSnapshot(ctx, nid, req)
			if err != nil {
				return
			}
			if resp.Success {
				responseChan <- struct {
					nodeID uint64
					resp   *ReadResponse
				}{nid, resp}
			}
		}(nodeID)
	}

	// Collect responses with timeout
	for i := 0; i < len(nodeIDs); i++ {
		select {
		case result := <-responseChan:
			responses[result.nodeID] = result.resp
		case <-ctx.Done():
			return responses, nil
		}
	}

	return responses, nil
}

// LocalSnapshotRead executes a snapshot read on the local database
// This is what gets called by the Reader interface implementation
func LocalSnapshotRead(db *sql.DB, snapshotTS hlc.Timestamp, tableName, query string, args []interface{}) (*ReadResponse, error) {
	// MVCC Snapshot Read Algorithm:
	// 1. Check for write intents (locks) on the rows
	// 2. If intent exists:
	//    - If intent's transaction is COMMITTED with commit_ts <= snapshot_ts, use that version
	//    - If intent's transaction is PENDING or commit_ts > snapshot_ts, read older version
	// 3. Read from __marmot__mvcc_versions where ts <= snapshot_ts (latest version before snapshot)
	// 4. If no MVCC version, read from base table (current data)

	// For now, implement simple version: read from base table
	// Full MVCC version resolution will be added when we have version history
	rows, err := db.QueryContext(context.Background(), query, args...)
	if err != nil {
		return &ReadResponse{
			Success: false,
			Error:   fmt.Sprintf("query failed: %v", err),
		}, nil
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return &ReadResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get columns: %v", err),
		}, nil
	}

	// Read all rows
	var results []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{}'s to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return &ReadResponse{
				Success: false,
				Error:   fmt.Sprintf("scan failed: %v", err),
			}, nil
		}

		// Build result map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			// Convert []byte to string for text columns
			if b, ok := val.([]byte); ok {
				rowMap[col] = string(b)
			} else {
				rowMap[col] = val
			}
		}
		results = append(results, rowMap)
	}

	if err := rows.Err(); err != nil {
		return &ReadResponse{
			Success: false,
			Error:   fmt.Sprintf("rows iteration error: %v", err),
		}, nil
	}

	return &ReadResponse{
		Success:  true,
		Rows:     results,
		RowCount: len(results),
	}, nil
}

// ReadWithWriteIntentCheck implements full MVCC snapshot read with write intent checking
// This will be used in the full integration
func ReadWithWriteIntentCheck(db *sql.DB, snapshotTS hlc.Timestamp, tableName, rowKey string) (map[string]interface{}, error) {
	// Step 1: Check for write intent on this row
	var intentTxnID uint64
	var intentTSWall, intentTSLogical int64
	err := db.QueryRow(`
		SELECT txn_id, ts_wall, ts_logical
		FROM __marmot__write_intents
		WHERE table_name = ? AND row_key = ?
	`, tableName, rowKey).Scan(&intentTxnID, &intentTSWall, &intentTSLogical)

	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to check write intent: %w", err)
	}

	if err == nil {
		// Write intent exists - check transaction status
		var txnStatus string
		var commitTSWall, commitTSLogical sql.NullInt64

		err := db.QueryRow(`
			SELECT status, commit_ts_wall, commit_ts_logical
			FROM __marmot__txn_records
			WHERE txn_id = ?
		`, intentTxnID).Scan(&txnStatus, &commitTSWall, &commitTSLogical)

		if err != nil {
			return nil, fmt.Errorf("failed to check transaction status: %w", err)
		}

		if txnStatus == "COMMITTED" && commitTSWall.Valid {
			commitTS := hlc.Timestamp{
				WallTime: commitTSWall.Int64,
				Logical:  int32(commitTSLogical.Int64),
			}

			// If committed before our snapshot, use this version
			if hlc.Compare(commitTS, snapshotTS) <= 0 {
				// Read from write intent's data snapshot
				var dataSnapshot []byte
				err := db.QueryRow(`
					SELECT data_snapshot
					FROM __marmot__write_intents
					WHERE table_name = ? AND row_key = ?
				`, tableName, rowKey).Scan(&dataSnapshot)

				if err != nil {
					return nil, fmt.Errorf("failed to read intent data: %w", err)
				}

				// Deserialize data snapshot
				var snapshotData map[string]interface{}
				err = json.Unmarshal(dataSnapshot, &snapshotData)
				if err != nil {
					return nil, fmt.Errorf("failed to deserialize intent snapshot: %w", err)
				}

				return snapshotData, nil
			}
		}

		// Intent is pending or committed after our snapshot - read older version
	}

	// Step 2: Read from MVCC versions (latest version <= snapshot_ts)
	var dataSnapshot []byte
	err = db.QueryRow(`
		SELECT data_snapshot
		FROM __marmot__mvcc_versions
		WHERE table_name = ? AND row_key = ? AND
		      (ts_wall < ? OR (ts_wall = ? AND ts_logical <= ?))
		ORDER BY ts_wall DESC, ts_logical DESC
		LIMIT 1
	`, tableName, rowKey, snapshotTS.WallTime, snapshotTS.WallTime, snapshotTS.Logical).Scan(&dataSnapshot)

	if err == sql.ErrNoRows {
		// No MVCC version - read from base table (current data)
		// This is the first read before any MVCC versions exist
		return map[string]interface{}{"_no_version": true}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("failed to read MVCC version: %w", err)
	}

	// Deserialize data snapshot
	var snapshotData map[string]interface{}
	err = json.Unmarshal(dataSnapshot, &snapshotData)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize MVCC snapshot: %w", err)
	}

	return snapshotData, nil
}
