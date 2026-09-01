package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/maxpert/marmot/protocol"
)

// describeColumnsTimeout bounds statement description. Describing prepares a
// statement and reads its metadata without stepping it, so it should be fast;
// the bound exists so a stuck connection cannot hang a client's PREPARE.
const describeColumnsTimeout = 5 * time.Second

// DescribeResultColumns reports the columns a statement returns, without
// running it, so COM_STMT_PREPARE can describe the result set.
//
// Clients build their column-name index from the prepare response. When a
// server reports no columns, a strict client (sqlx, for one) receives the rows
// but cannot address any column by name, which surfaces as a decode failure
// far from its cause.
//
// Statements that return nothing, and statements the database cannot prepare,
// yield no columns; the caller then omits the definitions.
func (h *CoordinatorHandler) DescribeResultColumns(session *protocol.ConnectionSession, sql string) ([]protocol.ColumnDef, error) {
	if session == nil || session.CurrentDatabase == "" {
		return nil, nil
	}
	if h.dbManager == nil {
		return nil, nil
	}

	replicatedDB, err := h.dbManager.GetReplicatedDatabase(session.CurrentDatabase)
	if err != nil {
		return nil, fmt.Errorf("failed to get database %s: %w", session.CurrentDatabase, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), describeColumnsTimeout)
	defer cancel()

	columns, err := replicatedDB.DescribeResultColumns(ctx, sql)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, nil
	}

	defs := make([]protocol.ColumnDef, len(columns))
	for i, col := range columns {
		defs[i] = protocol.ColumnDef{
			Name: col.Name,
			Type: protocol.ColumnTypeForDeclType(col.DeclType),
		}
	}
	return defs, nil
}
