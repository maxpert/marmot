package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/common"
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
		Statements:   convertStatementsToProto(req.Statements, req.Database),
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
func convertStatementsToProto(stmts []protocol.Statement, database string) []*Statement {
	protoStmts := make([]*Statement, len(stmts))
	for i, stmt := range stmts {
		// Use statement's database if set, otherwise use the transaction's database
		stmtDB := stmt.Database
		if stmtDB == "" {
			stmtDB = database
		}

		grpcType, ok := common.ToWireType(stmt.Type)
		if !ok {
			panic("convertStatementsToProto: unknown statement type")
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

		if isDML && (len(stmt.NewValues) > 0 || len(stmt.OldValues) > 0) {
			// CDC path: send row data instead of SQL
			protoStmt.Payload = &Statement_RowChange{
				RowChange: &RowChange{
					IntentKey: stmt.IntentKey,
					OldValues: stmt.OldValues,
					NewValues: stmt.NewValues,
				},
			}
		} else {
			// DDL or DML without CDC data: send SQL
			protoStmt.Payload = &Statement_DdlChange{
				DdlChange: &DDLChange{
					Sql: stmt.SQL,
				},
			}
		}

		protoStmts[i] = protoStmt
	}
	return protoStmts
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
