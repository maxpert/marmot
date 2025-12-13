package grpc

// Helper functions to access Statement data regardless of payload type (CDC vs SQL)

// GetIntentKey extracts intent key from either RowChange or falls back to empty
func (stmt *Statement) GetIntentKey() string {
	if rowChange := stmt.GetRowChange(); rowChange != nil {
		return rowChange.IntentKey
	}
	return ""
}

// GetSQL extracts SQL from DDLChange or returns empty if it's a RowChange
func (stmt *Statement) GetSQL() string {
	if ddlChange := stmt.GetDdlChange(); ddlChange != nil {
		return ddlChange.Sql
	}
	return ""
}

// HasCDCData returns true if statement has CDC row data
func (stmt *Statement) HasCDCData() bool {
	return stmt.GetRowChange() != nil
}

// HasSQL returns true if statement has SQL (DDL or fallback)
func (stmt *Statement) HasSQL() bool {
	return stmt.GetDdlChange() != nil
}
