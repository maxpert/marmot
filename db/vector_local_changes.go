package db

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
)

func (h *EngineHook) OnIndexLocalChanges(ctx context.Context, meta common.VectorIndexMeta, entries []common.CDCEntry) error {
	if len(entries) == 0 {
		return nil
	}

	h.localChangeMu.Lock()
	defer h.localChangeMu.Unlock()

	state, spec, err := h.ensureIndexState(ctx, meta)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	overlay := state.LoadOverlay()
	if overlay == nil {
		return fmt.Errorf("vector local changes: overlay not initialized for index %q", meta.IndexName)
	}

	mutations, err := h.buildOverlayMutations(meta, state, spec, entries)
	if err != nil {
		return err
	}
	if len(mutations) == 0 {
		return nil
	}
	pkColumn, err := h.primaryKeyColumn(meta.Database, meta.TableName)
	if err != nil {
		return err
	}
	var overlaySnapshotBefore *vecindex.OverlaySnapshot
	if overlay != nil {
		overlaySnapshotBefore = overlay.Snapshot()
	}
	if err := recordMaintenanceDeltas(meta, state, spec, pkColumn, overlaySnapshotBefore, entries); err != nil {
		return err
	}
	if err := overlay.ApplyCommittedBatch(mutations); err != nil {
		return fmt.Errorf("vector local changes: apply overlay batch: %w", err)
	}
	state.RecordRowsModified(uint64(countUniqueMutationRows(mutations)))
	return nil
}

func (h *EngineHook) OnIndexLoaded(ctx context.Context, meta common.VectorIndexMeta) error {
	state, _, err := h.ensureIndexState(ctx, meta)
	if err != nil {
		return err
	}
	if state != nil && state.ProbeVersion() == 0 {
		metricKind, err := metricFromString(meta.Metric)
		if err != nil {
			return err
		}
		h.startBootstrapWatcher(meta, vecindex.IVFSpec{
			ID:      meta.IndexName,
			Dim:     meta.Dim,
			Metric:  metricKind,
			Nlist:   meta.Nlist,
			Nprobe:  meta.Nprobe,
			MaxNorm: meta.MaxNorm,
			Seed:    StableIndexSeed(meta),
		})
	} else if state != nil && state.ProbeVersion() != 0 {
		h.startMaintenanceWatcher(meta)
	}
	return nil
}

func (h *EngineHook) ensureIndexState(ctx context.Context, meta common.VectorIndexMeta) (*vecindex.IndexState, vecindex.IVFSpec, error) {
	metricKind, err := metricFromString(meta.Metric)
	if err != nil {
		return nil, vecindex.IVFSpec{}, err
	}
	spec := vecindex.IVFSpec{
		ID:      meta.IndexName,
		Dim:     meta.Dim,
		Metric:  metricKind,
		Nlist:   meta.Nlist,
		Nprobe:  meta.Nprobe,
		MaxNorm: meta.MaxNorm,
		Seed:    StableIndexSeed(meta),
	}

	state, ok := h.engine.Lookup(meta.IndexName)
	if !ok {
		state = vecindex.NewIndexState(spec, nil)
		h.engine.Register(meta.IndexName, state)
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			return nil, spec, fmt.Errorf("vector local changes: get db path: %w", err)
		}
		conn, err := h.dbMgr.GetDatabaseConnection(meta.Database)
		if err != nil {
			return nil, spec, fmt.Errorf("vector local changes: get db connection: %w", err)
		}
		if err := BuildSegmentGenerationOnReopen(ctx, conn, dbPath, state, meta, spec); err != nil {
			return nil, spec, fmt.Errorf("vector local changes: reopen state: %w", err)
		}
	}
	if state.LoadOverlay() == nil {
		dbPath, err := h.dbMgr.GetDatabasePath(meta.Database)
		if err != nil {
			return nil, spec, fmt.Errorf("vector local changes: get db path: %w", err)
		}
		if err := openAndStoreOverlay(dbPath, meta.IndexName, state, state.ProbeVersion()); err != nil {
			return nil, spec, fmt.Errorf("vector local changes: open overlay: %w", err)
		}
	}
	if state.ProbeVersion() == 0 {
		h.startBootstrapWatcher(meta, spec)
	}
	return state, spec, nil
}

func (h *EngineHook) buildOverlayMutations(
	meta common.VectorIndexMeta,
	state *vecindex.IndexState,
	spec vecindex.IVFSpec,
	entries []common.CDCEntry,
) ([]vecindex.OverlayMutation, error) {
	pkColumn, err := h.primaryKeyColumn(meta.Database, meta.TableName)
	if err != nil {
		return nil, err
	}

	merged := mergeLocalCDCEntries(meta.TableName, entries)
	snapshot := (*vecindex.OverlaySnapshot)(nil)
	if overlay := state.LoadOverlay(); overlay != nil {
		snapshot = overlay.Snapshot()
	}
	nextSequence := uint64(1)
	if snapshot != nil {
		nextSequence = snapshot.LastSequence() + 1
	}
	epoch := state.ProbeVersion()

	mutations := make([]vecindex.OverlayMutation, 0, len(merged))
	appliedAtUnixNano := time.Now().UnixNano()
	for _, entry := range merged {
		oldRowID, oldRowOK, err := decodeCDCInt64(entry.OldValues, pkColumn)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode old rowid: %w", err)
		}
		newRowID, newRowOK, err := decodeCDCInt64(entry.NewValues, pkColumn)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode new rowid: %w", err)
		}
		if oldRowOK && newRowOK && oldRowID != newRowID {
			deleteMutation, used, err := buildDeleteMutation(state, epoch, oldRowID, nextSequence)
			if err != nil {
				return nil, err
			}
			if used {
				deleteMutation.AppliedAtUnixNano = appliedAtUnixNano
				mutations = append(mutations, deleteMutation)
				nextSequence++
			}
			entry.OldValues = nil
			oldRowOK = false
		}

		oldRaw, _, err := decodeCDCBytes(entry.OldValues, meta.ColumnName)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode old vector: %w", err)
		}
		newRaw, _, err := decodeCDCBytes(entry.NewValues, meta.ColumnName)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode new vector: %w", err)
		}
		if bytes.Equal(oldRaw, newRaw) {
			continue
		}

		switch {
		case len(newRaw) > 0:
			rowID := newRowID
			if !newRowOK {
				rowID = oldRowID
			}
			mutation, err := buildUpsertMutation(state, spec, epoch, rowID, newRaw, nextSequence)
			if err != nil {
				return nil, err
			}
			mutation.AppliedAtUnixNano = appliedAtUnixNano
			mutations = append(mutations, mutation)
			nextSequence++
		case oldRowOK:
			mutation, used, err := buildDeleteMutation(state, epoch, oldRowID, nextSequence)
			if err != nil {
				return nil, err
			}
			if used {
				mutation.AppliedAtUnixNano = appliedAtUnixNano
				mutations = append(mutations, mutation)
				nextSequence++
			}
		}
	}
	return mutations, nil
}

func countUniqueMutationRows(mutations []vecindex.OverlayMutation) int {
	if len(mutations) == 0 {
		return 0
	}
	seen := make(map[int64]struct{}, len(mutations))
	for _, mutation := range mutations {
		seen[mutation.RowID] = struct{}{}
	}
	return len(seen)
}

func recordMaintenanceDeltas(
	meta common.VectorIndexMeta,
	state *vecindex.IndexState,
	spec vecindex.IVFSpec,
	pkColumn string,
	overlaySnapshot *vecindex.OverlaySnapshot,
	entries []common.CDCEntry,
) error {
	if state == nil || state.ProbeState() == nil {
		return nil
	}
	merged := mergeLocalCDCEntries(meta.TableName, entries)
	for _, entry := range merged {
		oldRowID, oldRowOK, err := decodeCDCInt64(entry.OldValues, pkColumn)
		if err != nil {
			return fmt.Errorf("vector local changes: decode old rowid for maintenance: %w", err)
		}
		oldRaw, _, err := decodeCDCBytes(entry.OldValues, meta.ColumnName)
		if err != nil {
			return fmt.Errorf("vector local changes: decode old vector for maintenance: %w", err)
		}
		newRaw, _, err := decodeCDCBytes(entry.NewValues, meta.ColumnName)
		if err != nil {
			return fmt.Errorf("vector local changes: decode new vector for maintenance: %w", err)
		}
		if bytes.Equal(oldRaw, newRaw) {
			continue
		}
		oldCluster := int64(0)
		if oldRowOK {
			oldCluster = currentRowCluster(state, overlaySnapshot, oldRowID)
		}
		_, oldVec, err := maintenancePreparedCluster(state, spec, oldRaw)
		if err != nil {
			return err
		}
		if oldCluster == 0 {
			oldCluster, _, err = maintenancePreparedCluster(state, spec, oldRaw)
			if err != nil {
				return err
			}
		}
		newCluster, newVec, err := maintenancePreparedCluster(state, spec, newRaw)
		if err != nil {
			return err
		}
		state.RecordClusterMutation(oldCluster, oldVec, newCluster, newVec)
	}
	return nil
}

func currentRowCluster(state *vecindex.IndexState, overlaySnapshot *vecindex.OverlaySnapshot, rowID int64) int64 {
	if state == nil || rowID == 0 {
		return 0
	}
	if overlaySnapshot != nil {
		if clusterID, ok := overlaySnapshot.RowCluster(rowID); ok {
			return clusterID
		}
	}
	segment := state.LoadSegmentStore()
	if segment == nil || segment.RowMap == nil {
		return 0
	}
	loc, ok, err := segment.RowMap.Lookup(rowID)
	if err != nil || !ok {
		return 0
	}
	return loc.ClusterID
}

func maintenancePreparedCluster(state *vecindex.IndexState, spec vecindex.IVFSpec, raw []byte) (int64, []float32, error) {
	if len(raw) == 0 {
		return 0, nil, nil
	}
	prepared, err := materializeVectorBlob(raw, spec.Metric, spec.Dim, spec.MaxNorm)
	if err != nil {
		return 0, nil, fmt.Errorf("vector local changes: materialize maintenance vector: %w", err)
	}
	if prepared == nil {
		return 0, nil, nil
	}
	clusterID := int64(0)
	if cs := state.ProbeState(); cs != nil {
		clusterID, err = assignPreparedAgainstSet(prepared, spec, cs)
		if err != nil {
			return 0, nil, fmt.Errorf("vector local changes: assign maintenance vector: %w", err)
		}
	}
	return clusterID, append([]float32(nil), metric.BytesToFloat32(prepared)...), nil
}

func buildUpsertMutation(
	state *vecindex.IndexState,
	spec vecindex.IVFSpec,
	epoch uint64,
	rowID int64,
	raw []byte,
	sequence uint64,
) (vecindex.OverlayMutation, error) {
	prepared, err := materializeVectorBlob(raw, spec.Metric, spec.Dim, spec.MaxNorm)
	if err != nil {
		return vecindex.OverlayMutation{}, fmt.Errorf("vector local changes: materialize rowid %d: %w", rowID, err)
	}
	if prepared == nil {
		return vecindex.OverlayMutation{}, fmt.Errorf("vector local changes: materialized vector is nil for rowid %d", rowID)
	}

	clusterID := int64(0)
	if cs := state.ProbeState(); cs != nil {
		clusterID, err = assignPreparedAgainstSet(prepared, spec, cs)
		if err != nil {
			return vecindex.OverlayMutation{}, fmt.Errorf("vector local changes: assign rowid %d: %w", rowID, err)
		}
	}

	kind := vecindex.OverlayMutationUpsert
	if stableRowExists(state, rowID) {
		kind = vecindex.OverlayMutationReplace
	}
	return vecindex.OverlayMutation{
		Kind:      kind,
		Epoch:     epoch,
		Sequence:  sequence,
		ClusterID: clusterID,
		RowID:     rowID,
		Vec:       prepared,
	}, nil
}

func buildDeleteMutation(
	state *vecindex.IndexState,
	epoch uint64,
	rowID int64,
	sequence uint64,
) (vecindex.OverlayMutation, bool, error) {
	if rowID == 0 {
		return vecindex.OverlayMutation{}, false, nil
	}
	if !stableRowExists(state, rowID) {
		if overlay := state.LoadOverlay(); overlay != nil {
			snapshot := overlay.Snapshot()
			if snapshot != nil {
				if _, ok := snapshot.RowCluster(rowID); !ok && !snapshot.HasTombstone(rowID) {
					return vecindex.OverlayMutation{}, false, nil
				}
			}
		}
	}
	return vecindex.OverlayMutation{
		Kind:      vecindex.OverlayMutationDelete,
		Epoch:     epoch,
		Sequence:  sequence,
		ClusterID: 0,
		RowID:     rowID,
	}, true, nil
}

func stableRowExists(state *vecindex.IndexState, rowID int64) bool {
	if state == nil {
		return false
	}
	segment := state.LoadSegmentStore()
	if segment == nil || segment.RowMap == nil {
		return false
	}
	_, ok, err := segment.RowMap.Lookup(rowID)
	return err == nil && ok
}

type mergedLocalCDCEntry struct {
	OldValues map[string][]byte
	NewValues map[string][]byte
}

func mergeLocalCDCEntries(tableName string, entries []common.CDCEntry) []mergedLocalCDCEntry {
	ordered := make([]string, 0, len(entries))
	merged := make(map[string]*mergedLocalCDCEntry, len(entries))
	for _, entry := range entries {
		if entry.Table != tableName {
			continue
		}
		key := string(entry.IntentKey)
		current, ok := merged[key]
		if !ok {
			current = &mergedLocalCDCEntry{
				OldValues: copyCDCValues(entry.OldValues),
				NewValues: copyCDCValues(entry.NewValues),
			}
			merged[key] = current
			ordered = append(ordered, key)
			continue
		}
		if current.OldValues == nil && len(entry.OldValues) > 0 {
			current.OldValues = make(map[string][]byte, len(entry.OldValues))
		}
		if current.NewValues == nil && len(entry.NewValues) > 0 {
			current.NewValues = make(map[string][]byte, len(entry.NewValues))
		}
		mergeCDCValueMap(current.OldValues, entry.OldValues)
		mergeCDCValueMap(current.NewValues, entry.NewValues)
	}

	result := make([]mergedLocalCDCEntry, 0, len(ordered))
	for _, key := range ordered {
		result = append(result, *merged[key])
	}
	return result
}

func copyCDCValues(src map[string][]byte) map[string][]byte {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string][]byte, len(src))
	for key, value := range src {
		out[key] = append([]byte(nil), value...)
	}
	return out
}

func mergeCDCValueMap(dst map[string][]byte, src map[string][]byte) {
	if len(src) == 0 {
		return
	}
	if dst == nil {
		return
	}
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		dst[key] = append([]byte(nil), src[key]...)
	}
}

func (h *EngineHook) primaryKeyColumn(database, table string) (string, error) {
	replicatedDB, err := h.dbMgr.GetDatabase(database)
	if err != nil {
		return "", fmt.Errorf("vector local changes: get replicated db: %w", err)
	}
	schema, err := replicatedDB.GetCachedTableSchema(table)
	if err != nil {
		return "", fmt.Errorf("vector local changes: get schema for %s: %w", table, err)
	}
	if len(schema.PrimaryKeys) != 1 {
		return "", fmt.Errorf("vector local changes: expected exactly one primary key for %s, got %d", table, len(schema.PrimaryKeys))
	}
	return schema.PrimaryKeys[0], nil
}

func decodeCDCInt64(values map[string][]byte, column string) (int64, bool, error) {
	raw, ok := values[column]
	if !ok {
		return 0, false, nil
	}
	var value any
	if err := encoding.Unmarshal(raw, &value); err != nil {
		return 0, false, err
	}
	switch v := value.(type) {
	case int64:
		return v, true, nil
	case int:
		return int64(v), true, nil
	case uint64:
		return int64(v), true, nil
	case uint32:
		return int64(v), true, nil
	default:
		return 0, false, fmt.Errorf("unexpected rowid type %T", value)
	}
}

func decodeCDCBytes(values map[string][]byte, column string) ([]byte, bool, error) {
	raw, ok := values[column]
	if !ok {
		return nil, false, nil
	}
	var value any
	if err := encoding.Unmarshal(raw, &value); err != nil {
		return nil, false, err
	}
	switch v := value.(type) {
	case nil:
		return nil, true, nil
	case []byte:
		return append([]byte(nil), v...), true, nil
	case string:
		return []byte(v), true, nil
	default:
		return nil, false, fmt.Errorf("unexpected vector value type %T", value)
	}
}
