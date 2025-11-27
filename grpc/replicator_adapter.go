package grpc

import (
	"context"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
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
		Phase:          convertPhaseToProto(req.Phase),
		Database:       req.Database,
		MutationGuards: convertMutationGuardsToProto(req.MutationGuards),
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

		protoStmt := &Statement{
			Type:      convertStatementTypeToProto(stmt.Type),
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
					RowKey:    stmt.RowKey,
					OldValues: stmt.OldValues,
					NewValues: stmt.NewValues,
				},
			}
		} else {
			// DDL or DML without CDC data: send SQL
			log.Debug().
				Str("sql_prefix", func() string {
					if len(stmt.SQL) > 50 {
						return stmt.SQL[:50]
					}
					return stmt.SQL
				}()).
				Bool("is_ddl", !isDML).
				Msg("Sending SQL to remote node (DDL or DML fallback)")

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

// convertStatementTypeToProto converts protocol.StatementType to gRPC StatementType
func convertStatementTypeToProto(stmtType protocol.StatementType) StatementType {
	switch stmtType {
	case protocol.StatementInsert:
		return StatementType_INSERT
	case protocol.StatementUpdate:
		return StatementType_UPDATE
	case protocol.StatementDelete:
		return StatementType_DELETE
	case protocol.StatementReplace:
		return StatementType_REPLACE
	case protocol.StatementDDL:
		return StatementType_DDL
	case protocol.StatementCreateDatabase:
		return StatementType_CREATE_DATABASE
	case protocol.StatementDropDatabase:
		return StatementType_DROP_DATABASE
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

// convertMutationGuardsToProto converts coordinator.MutationGuard map to gRPC MutationGuard map
func convertMutationGuardsToProto(guards map[string]*coordinator.MutationGuard) map[string]*MutationGuard {
	if guards == nil {
		return nil
	}

	result := make(map[string]*MutationGuard, len(guards))
	for table, guard := range guards {
		result[table] = &MutationGuard{
			Filter:           guard.Filter,
			ExpectedRowCount: guard.ExpectedRowCount,
		}
	}
	return result
}
