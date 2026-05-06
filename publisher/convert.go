package publisher

import "github.com/maxpert/marmot/common"

// ConvertToCDCEvents converts common.CDCEntry to publisher.CDCEvent format
func ConvertToCDCEvents(txnID uint64, database string, entries []common.CDCEntry, commitTSNanos int64, nodeID uint64) []CDCEvent {
	events := make([]CDCEvent, 0, len(entries))
	commitTSMillis := commitTSNanos / 1_000_000 // Convert nanoseconds to milliseconds

	for _, entry := range entries {
		// Determine operation type
		var operation uint8
		switch entry.Operation {
		case 0:
			if len(entry.NewValues) > 0 {
				operation = OpInsert
			}
		case 1:
			operation = OpInsert // SQLite REPLACE publishes as an insert-style after image.
		case 2:
			operation = OpUpdate
		case 3:
			operation = OpDelete
		default:
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
		}
		if operation == 0 {
			operation = OpInsert
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
