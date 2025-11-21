package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// GRPCReplicator implements coordinator.Replicator using gRPC client
// It adapts between coordinator types and gRPC protobuf types
type GRPCReplicator struct {
	client *Client
}

// NewGRPCReplicator creates a new gRPC replicator adapter
func NewGRPCReplicator(client *Client) coordinator.Replicator {
	return &GRPCReplicator{
		client: client,
	}
}

// ReplicateTransaction implements coordinator.Replicator
func (gr *GRPCReplicator) ReplicateTransaction(ctx context.Context, nodeID uint64, req *coordinator.ReplicationRequest) (*coordinator.ReplicationResponse, error) {
	// Convert coordinator.ReplicationRequest to gRPC TransactionRequest
	grpcReq := &TransactionRequest{
		TxnId:        req.TxnID,
		SourceNodeId: req.NodeID,
		Statements:   convertStatementsToProto(req.Statements),
		Timestamp: &HLC{
			WallTime: req.StartTS.WallTime,
			Logical:  req.StartTS.Logical,
			NodeId:   req.StartTS.NodeID,
		},
		Phase: convertPhaseToProto(req.Phase),
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

// convertStatementsToProto converts protocol.Statement to gRPC Statement
func convertStatementsToProto(stmts []protocol.Statement) []*Statement {
	protoStmts := make([]*Statement, len(stmts))
	for i, stmt := range stmts {
		protoStmts[i] = &Statement{
			Sql:       stmt.SQL,
			Type:      convertStatementTypeToProto(stmt.Type),
			TableName: stmt.TableName,
		}
	}
	return protoStmts
}

// convertStatementTypeToProto converts protocol.StatementType to gRPC StatementType
func convertStatementTypeToProto(stmtType protocol.StatementType) StatementType {
	switch stmtType {
	case protocol.StatementInsert:
		return StatementType_INSERT
	case protocol.StatementUpdate:
		return StatementType_UPDATE
	case protocol.StatementDelete:
		return StatementType_DELETE
	default:
		return StatementType_INSERT
	}
}

// convertPhaseToProto converts coordinator.ReplicationPhase to gRPC TransactionPhase
func convertPhaseToProto(phase coordinator.ReplicationPhase) TransactionPhase {
	switch phase {
	case coordinator.PhasePrep:
		return TransactionPhase_PREPARE
	case coordinator.PhaseCommit:
		return TransactionPhase_COMMIT
	case coordinator.PhaseAbort:
		return TransactionPhase_ABORT
	default:
		return TransactionPhase_PREPARE
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
