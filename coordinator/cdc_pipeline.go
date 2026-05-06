package coordinator

import (
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
)

// ConvertToStatement converts a common.CDCEntry to protocol.Statement
func ConvertToStatement(entry common.CDCEntry) protocol.Statement {
	var stmtType protocol.StatementCode
	switch entry.Operation {
	case 1:
		stmtType = protocol.StatementReplace
	case 2:
		stmtType = protocol.StatementUpdate
	case 3:
		stmtType = protocol.StatementDelete
	default:
		hasOldValues := len(entry.OldValues) > 0
		hasNewValues := len(entry.NewValues) > 0
		if hasOldValues && hasNewValues {
			stmtType = protocol.StatementUpdate
		} else if hasNewValues {
			stmtType = protocol.StatementInsert
		} else if hasOldValues {
			stmtType = protocol.StatementDelete
		} else {
			stmtType = protocol.StatementInsert
		}
	}

	return protocol.Statement{
		Type:         stmtType,
		TableName:    entry.Table,
		IntentKey:    entry.IntentKey,
		OldValues:    entry.OldValues,
		NewValues:    entry.NewValues,
		Operation:    entry.Operation,
		EncodedRow:   entry.EncodedRow,
		EncodedCodec: entry.EncodedCodec,
	}
}
