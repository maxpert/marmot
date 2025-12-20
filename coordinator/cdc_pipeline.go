package coordinator

import (
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
)

// ConvertToStatement converts a common.CDCEntry to protocol.Statement
func ConvertToStatement(entry common.CDCEntry) protocol.Statement {
	hasOldValues := len(entry.OldValues) > 0
	hasNewValues := len(entry.NewValues) > 0

	var stmtType protocol.StatementCode
	if hasOldValues && hasNewValues {
		stmtType = protocol.StatementUpdate
	} else if hasNewValues {
		stmtType = protocol.StatementInsert
	} else if hasOldValues {
		stmtType = protocol.StatementDelete
	} else {
		stmtType = protocol.StatementInsert
	}

	return protocol.Statement{
		Type:      stmtType,
		TableName: entry.Table,
		IntentKey: entry.IntentKey,
		OldValues: entry.OldValues,
		NewValues: entry.NewValues,
	}
}
