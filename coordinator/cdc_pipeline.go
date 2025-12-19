package coordinator

import (
	"fmt"
	"sort"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/protocol"
	"github.com/rs/zerolog/log"
)

// OpType constants define CDC operation types, matching db/meta_schema.go values.
const (
	OpTypeInsert uint8 = 0 // INSERT: new row creation
	OpTypeUpdate uint8 = 2 // UPDATE: existing row modification
	OpTypeDelete uint8 = 3 // DELETE: row removal
)

// CDCPipelineConfig configures the CDC processing pipeline
type CDCPipelineConfig struct {
	ValidateEntries bool // Enable strict validation (default true)
}

// DefaultCDCPipelineConfig returns the default configuration for CDC pipeline
func DefaultCDCPipelineConfig() CDCPipelineConfig {
	return CDCPipelineConfig{
		ValidateEntries: true,
	}
}

// CDCPipelineResult contains the processed output
type CDCPipelineResult struct {
	Statements   []protocol.Statement // All statements ready for replication
	TotalEntries int                  // Input count
	MergedCount  int                  // Output count (after merge)
	DroppedCount int                  // Cancelled out (INSERT→DELETE)
}

// ValidateCDCEntry validates a single CDC entry for required fields
func ValidateCDCEntry(entry common.CDCEntry) error {
	if entry.Table == "" {
		return fmt.Errorf("TableName is required for all CDC entries")
	}

	if len(entry.IntentKey) == 0 {
		return fmt.Errorf("IntentKey is required for all CDC operations")
	}

	hasOldValues := len(entry.OldValues) > 0
	hasNewValues := len(entry.NewValues) > 0

	if !hasNewValues && !hasOldValues {
		return fmt.Errorf("entry must have either OldValues or NewValues")
	}

	return nil
}

// GroupByIntentKey groups CDC entries by intent key.
// Binary intent keys already contain table prefix, so direct conversion is safe.
func GroupByIntentKey(entries []common.CDCEntry) map[string][]common.CDCEntry {
	groups := make(map[string][]common.CDCEntry)

	for _, entry := range entries {
		key := string(entry.IntentKey)
		groups[key] = append(groups[key], entry)
	}

	return groups
}

// MergeGroup merges a sequence of CDC entries for the same row key
// Returns (merged entry, wasDropped)
func MergeGroup(entries []common.CDCEntry) (*common.CDCEntry, bool) {
	if len(entries) == 0 {
		return nil, false
	}

	if len(entries) == 1 {
		return &entries[0], false
	}

	// Multiple entries - need to merge
	first := entries[0]
	last := entries[len(entries)-1]

	firstHasOld := len(first.OldValues) > 0
	firstHasNew := len(first.NewValues) > 0
	lastHasOld := len(last.OldValues) > 0
	lastHasNew := len(last.NewValues) > 0

	// INSERT → DELETE: Cancelled out, drop this entry
	if !firstHasOld && firstHasNew && lastHasOld && !lastHasNew {
		return nil, true
	}

	// DELETE → INSERT: UPSERT becomes UPDATE
	// OldValues from DELETE, NewValues from INSERT
	if firstHasOld && !firstHasNew && !lastHasOld && lastHasNew {
		merged := &common.CDCEntry{
			Table:     first.Table,
			IntentKey: first.IntentKey,
			OldValues: first.OldValues,
			NewValues: last.NewValues,
		}
		return merged, false
	}

	// UPDATE → UPDATE: OldValues from FIRST, NewValues from LAST
	if firstHasOld && firstHasNew && lastHasOld && lastHasNew {
		merged := &common.CDCEntry{
			Table:     first.Table,
			IntentKey: first.IntentKey,
			OldValues: first.OldValues,
			NewValues: last.NewValues,
		}
		return merged, false
	}

	// UPDATE → DELETE: Becomes DELETE with OldValues from first UPDATE
	if firstHasOld && firstHasNew && lastHasOld && !lastHasNew {
		merged := &common.CDCEntry{
			Table:     first.Table,
			IntentKey: first.IntentKey,
			OldValues: first.OldValues,
			NewValues: make(map[string][]byte),
		}
		return merged, false
	}

	// INSERT → UPDATE: Keep as INSERT with final values
	// Row didn't exist before transaction, should exist with final values after
	if !firstHasOld && firstHasNew && lastHasOld && lastHasNew {
		merged := &common.CDCEntry{
			Table:     first.Table,
			IntentKey: first.IntentKey,
			OldValues: make(map[string][]byte),
			NewValues: last.NewValues,
		}
		return merged, false
	}

	// Default: Use standard merge logic (OldValues from all, NewValues from all)
	merged := &common.CDCEntry{
		Table:     first.Table,
		IntentKey: first.IntentKey,
		OldValues: make(map[string][]byte),
		NewValues: make(map[string][]byte),
	}

	for _, entry := range entries {
		for k, v := range entry.OldValues {
			merged.OldValues[k] = v
		}
		for k, v := range entry.NewValues {
			merged.NewValues[k] = v
		}
	}

	return merged, false
}

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

// ProcessCDCEntries is the main orchestrator for CDC pipeline processing
func ProcessCDCEntries(entries []common.CDCEntry, config CDCPipelineConfig) (*CDCPipelineResult, error) {
	result := &CDCPipelineResult{
		Statements:   make([]protocol.Statement, 0),
		TotalEntries: len(entries),
		MergedCount:  0,
		DroppedCount: 0,
	}

	if len(entries) == 0 {
		return result, nil
	}

	// Validate entries if enabled
	if config.ValidateEntries {
		for i, entry := range entries {
			if err := ValidateCDCEntry(entry); err != nil {
				return nil, fmt.Errorf("validation failed for entry %d (table=%s, intentKey=%s): %w",
					i, entry.Table, entry.IntentKey, err)
			}
		}
	}

	// Group by intent key
	groups := GroupByIntentKey(entries)

	// Sort group keys for deterministic output
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Process each group in sorted order
	for _, key := range keys {
		group := groups[key]
		merged, wasDropped := MergeGroup(group)
		if wasDropped {
			result.DroppedCount++
			continue
		}

		if merged != nil {
			stmt := ConvertToStatement(*merged)
			result.Statements = append(result.Statements, stmt)
			result.MergedCount++
		}
	}

	if len(result.Statements) > 0 || result.DroppedCount > 0 {
		log.Debug().
			Int("total_entries", result.TotalEntries).
			Int("merged_count", result.MergedCount).
			Int("dropped_count", result.DroppedCount).
			Msg("CDC pipeline processing complete")
	}

	return result, nil
}
