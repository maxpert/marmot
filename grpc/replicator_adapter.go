package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// GRPCReplicator implements coordinator.Replicator and coordinator.StreamReplicator
// using gRPC client. It adapts between coordinator types and gRPC protobuf types.
type GRPCReplicator struct {
	client *Client
}

// Compile-time interface check
var _ coordinator.StreamReplicator = (*GRPCReplicator)(nil)

// NewGRPCReplicator creates a new gRPC replicator adapter
func NewGRPCReplicator(client *Client) *GRPCReplicator {
	return &GRPCReplicator{
		client: client,
	}
}

// CleanupStagedPayload removes staged load-data payloads for a completed transaction.
func (gr *GRPCReplicator) CleanupStagedPayload(txnID uint64) {
	cleanupStagedLoadDataByTxn(txnID)
}

// ReplicateTransaction implements coordinator.Replicator
func (gr *GRPCReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	statements, err := convertStatementsToProto(req.Statements, req.Database, req.TxnID)
	if err != nil {
		return nil, err
	}
	// Convert coordinator.ReplicationRequest to gRPC TransactionRequest
	grpcReq := &TransactionRequest{
		TxnId:        req.TxnID,
		SourceNodeId: req.NodeID,
		Statements:   statements,
		Timestamp: &HLC{
			WallTime: req.StartTS.WallTime,
			Logical:  req.StartTS.Logical,
			NodeId:   req.StartTS.NodeID,
		},
		Phase:                 convertPhaseToProto(req.Phase),
		Database:              req.Database,
		RequiredSchemaVersion: req.RequiredSchemaVersion,
	}

	// Call gRPC client
	grpcResp, err := gr.client.ReplicateTransaction(ctx, nodeID, grpcReq)
	if err != nil {
		return nil, fmt.Errorf("gRPC call failed: %w", err)
	}

	// Convert gRPC TransactionResponse to coordinator.ReplicationResponse
	resp := &coordinator.ReplicationResponse{
		Success:          grpcResp.Success,
		Error:            grpcResp.ErrorMessage,
		ConflictDetected: grpcResp.ConflictDetected,
		ConflictDetails:  grpcResp.ConflictDetails,
	}

	return resp, nil
}

// convertStatementsToProto converts protocol.Statement to gRPC Statement with CDC
func convertStatementsToProto(stmts []protocol.Statement, database string, txnID uint64) ([]*Statement, error) {
	protoStmts := make([]*Statement, len(stmts))
	for i, stmt := range stmts {
		// Use statement's database if set, otherwise use the transaction's database
		stmtDB := stmt.Database
		if stmtDB == "" {
			stmtDB = database
		}

		grpcType, ok := common.ToWireType(stmt.Type)
		if !ok {
			return nil, fmt.Errorf("convertStatementsToProto: unknown statement type %v", stmt.Type)
		}
		protoStmt := &Statement{
			Type:      grpcType,
			TableName: stmt.TableName,
			Database:  stmtDB,
		}

		// For DML operations: send CDC row data
		// For DDL operations: send SQL
		isDML := stmt.Type == protocol.StatementInsert ||
			stmt.Type == protocol.StatementUpdate ||
			stmt.Type == protocol.StatementDelete ||
			stmt.Type == protocol.StatementReplace

		switch {
		case stmt.Type == protocol.StatementLoadData:
			loadID := fmt.Sprintf("%d:%d", txnID, i)
			stageLoadDataPayload(loadID, stmt.LoadDataPayload)
			protoStmt.Payload = &Statement_LoadDataChange{
				LoadDataChange: &LoadDataChange{
					Sql:        stmt.SQL,
					LoadId:     loadID,
					DataSize:   uint64(len(stmt.LoadDataPayload)),
					ChunkBytes: 256 * 1024,
				},
			}
		case isDML && (len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0):
			// CDC path: send row data instead of SQL
			protoStmt.Payload = &Statement_RowChange{
				RowChange: &RowChange{
					IntentKey: stmt.IntentKey,
					OldValues: stmt.OldValues,
					NewValues: stmt.NewValues,
				},
			}
		default:
			// DDL or DML without CDC data: send SQL
			protoStmt.Payload = &Statement_DdlChange{
				DdlChange: &DDLChange{
					Sql: stmt.SQL,
				},
			}
		}

		protoStmts[i] = protoStmt
	}

	return protoStmts, nil
}

// convertPhaseToProto converts coordinator.ReplicationPhase to gRPC TransactionPhase.
// Panics on unknown phase - internal consistency error.
func convertPhaseToProto(phase coordinator.ReplicationPhase) TransactionPhase {
	switch phase {
	case coordinator.PhasePrep:
		return TransactionPhase_PREPARE
	case coordinator.PhaseCommit:
		return TransactionPhase_COMMIT
	case coordinator.PhaseAbort:
		return TransactionPhase_ABORT
	default:
		panic(fmt.Sprintf("convertPhaseToProto: unknown replication phase %d", phase))
	}
}

// convertTimestampToHLC converts hlc.Timestamp to gRPC HLC
func convertTimestampToHLC(ts hlc.Timestamp) *HLC {
	return &HLC{
		WallTime: ts.WallTime,
		Logical:  ts.Logical,
		NodeId:   ts.NodeID,
	}
}

// StreamReplicateTransaction implements coordinator.StreamReplicator.
// Uses gRPC streaming for large CDC payloads (≥128KB).
func (gr *GRPCReplicator) StreamReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	statements, err := convertStatementsToProto(req.Statements, req.Database, req.TxnID)
	if err != nil {
		return nil, err
	}

	timestamp := convertTimestampToHLC(req.StartTS)

	// Call streaming client with configured chunk size
	chunkSize := coordinator.GetStreamChunkSize()
	grpcResp, err := gr.client.TransactionStream(ctx, nodeID, req.TxnID, req.Database, statements, chunkSize, timestamp, req.NodeID)
	if err != nil {
		return nil, fmt.Errorf("streaming gRPC call failed: %w", err)
	}

	// Convert gRPC TransactionResponse to coordinator.ReplicationResponse
	resp := &coordinator.ReplicationResponse{
		Success:          grpcResp.Success,
		Error:            grpcResp.ErrorMessage,
		ConflictDetected: grpcResp.ConflictDetected,
		ConflictDetails:  grpcResp.ConflictDetails,
	}

	return resp, nil
}
