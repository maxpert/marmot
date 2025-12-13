package publisher

// ConvertCDCEntry converts a generic CDC entry map to CDCEvent
// This avoids import cycles by accepting a flexible input type
type GenericCDCEntry struct {
	Table     string
	IntentKey string
	OldValues map[string][]byte
	NewValues map[string][]byte
}

// ConvertToCDCEvents converts generic CDC entries to publisher.CDCEvent format
func ConvertToCDCEvents(txnID uint64, database string, entries []GenericCDCEntry, commitTSNanos int64, nodeID uint64) []CDCEvent {
	events := make([]CDCEvent, 0, len(entries))
	commitTSMillis := commitTSNanos / 1_000_000 // Convert nanoseconds to milliseconds

	for _, entry := range entries {
		// Determine operation type
		var operation uint8
		hasOld := len(entry.OldValues) > 0
		hasNew := len(entry.NewValues) > 0

		if hasNew && !hasOld {
			operation = OpInsert
		} else if hasNew && hasOld {
			operation = OpUpdate
		} else if !hasNew && hasOld {
			operation = OpDelete
		} else {
			continue // Skip invalid entries
		}

		event := CDCEvent{
			SeqNum:    0, // Will be assigned by PublishLog
			TxnID:     txnID,
			Database:  database,
			Table:     entry.Table,
			Operation: operation,
			IntentKey: entry.IntentKey,
			Before:    entry.OldValues,
			After:     entry.NewValues,
			CommitTS:  commitTSMillis,
			NodeID:    nodeID,
		}
		events = append(events, event)
	}

	return events
}
