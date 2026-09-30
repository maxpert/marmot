package db

// statementInsertID returns the insert id one statement reports to its client:
// the value of the target table's AUTO_INCREMENT column in the first row that
// statement inserted, read by column name from that statement's first insert
// CDC entry.
//
// It is deliberately NOT SQLite's sqlite3_last_insert_rowid(), which is the
// LAST row of a multi-row insert and lives on the connection rather than on the
// statement, and NOT "the row's primary key", which need not be the
// auto-increment column.
//
// It returns 0 when the statement inserted no rows and when the table has no
// auto-increment column. 0 is what leaves a client's LAST_INSERT_ID() unchanged,
// which is MySQL's documented behaviour when no rows are successfully inserted.
//
// That only holds because every writer of the session field drops a zero, and
// there are two call paths, not one: the coordinator's OK-packet paths in
// protocol/server.go and a replica's forwarded responses in
// replica/handler.go (applyForwardedSessionState). Both go through
// protocol.ConnectionSession.RecordInsertId, which is the single home of the
// rule; the replica path stored zeros unconditionally until that was fixed, so
// do not assume a new writer inherits the behaviour - route it through
// RecordInsertId.
func statementInsertID(schemaCache *SchemaCache, entries []*IntentEntry) int64 {
	if schemaCache == nil {
		return 0
	}
	for _, entry := range entries {
		if entry == nil || entry.Operation != uint8(OpTypeInsert) {
			continue
		}
		schema, err := schemaCache.GetSchemaFor(entry.Table)
		if err != nil || schema == nil {
			return 0
		}
		column := schema.GetAutoIncrementCol()
		if column == "" {
			return 0
		}
		id, ok, err := decodeCDCInt64(entry.NewValues, column)
		if err != nil || !ok {
			return 0
		}
		return id
	}
	return 0
}
