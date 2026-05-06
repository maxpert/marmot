package db

import (
	"bytes"
	"context"
	stdbinary "encoding/binary"
	"fmt"
	"time"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/metric"
	"github.com/maxpert/marmot/modules/vecindex/pkg/quantize"
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

	snapshot := (*vecindex.OverlaySnapshot)(nil)
	if overlay := state.LoadOverlay(); overlay != nil {
		snapshot = overlay.Snapshot()
	}
	nextSequence := uint64(1)
	if snapshot != nil {
		nextSequence = snapshot.LastSequence() + 1
	}
	epoch := state.ProbeVersion()

	mutations := make([]vecindex.OverlayMutation, 0, len(entries))
	batchLiveRows := make(map[int64]struct{}, len(entries))
	appliedAtUnixNano := time.Now().UnixNano()
	for _, entry := range entries {
		if entry.Table != meta.TableName {
			continue
		}
		oldRowID, oldRowOK, err := decodeCDCInt64(entry.OldValues, pkColumn)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode old rowid: %w", err)
		}
		newRowID, newRowOK, err := decodeCDCInt64(entry.NewValues, pkColumn)
		if err != nil {
			return nil, fmt.Errorf("vector local changes: decode new rowid: %w", err)
		}
		if oldRowOK && newRowOK && oldRowID != newRowID {
			_, batchHadOldRow := batchLiveRows[oldRowID]
			deleteMutation, used, err := buildDeleteMutation(state, epoch, oldRowID, nextSequence, batchHadOldRow)
			if err != nil {
				return nil, err
			}
			if used {
				deleteMutation.AppliedAtUnixNano = appliedAtUnixNano
				deleteMutation.CommitTxnID = entry.CommitTxnID
				deleteMutation.CommitSeqNum = entry.CommitSeqNum
				mutations = append(mutations, deleteMutation)
				nextSequence++
			}
			delete(batchLiveRows, oldRowID)
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
			mutation.CommitTxnID = entry.CommitTxnID
			mutation.CommitSeqNum = entry.CommitSeqNum
			mutations = append(mutations, mutation)
			batchLiveRows[rowID] = struct{}{}
			nextSequence++
		case oldRowOK:
			_, batchHadOldRow := batchLiveRows[oldRowID]
			mutation, used, err := buildDeleteMutation(state, epoch, oldRowID, nextSequence, batchHadOldRow)
			if err != nil {
				return nil, err
			}
			if used {
				mutation.AppliedAtUnixNano = appliedAtUnixNano
				mutation.CommitTxnID = entry.CommitTxnID
				mutation.CommitSeqNum = entry.CommitSeqNum
				mutations = append(mutations, mutation)
				delete(batchLiveRows, oldRowID)
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
	batchRows := make(map[int64]maintenanceBatchRow, len(entries))
	for _, entry := range entries {
		if entry.Table != meta.TableName {
			continue
		}
		oldRowID, oldRowOK, err := decodeCDCInt64(entry.OldValues, pkColumn)
		if err != nil {
			return fmt.Errorf("vector local changes: decode old rowid for maintenance: %w", err)
		}
		newRowID, newRowOK, err := decodeCDCInt64(entry.NewValues, pkColumn)
		if err != nil {
			return fmt.Errorf("vector local changes: decode new rowid for maintenance: %w", err)
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
		var oldVec []float32
		if oldRowOK {
			if current, ok := batchRows[oldRowID]; ok {
				if current.live {
					oldCluster = current.cluster
					oldVec = current.vec
				}
			} else {
				oldCluster = currentRowCluster(state, overlaySnapshot, oldRowID)
				assignedCluster, preparedVec, err := maintenancePreparedCluster(state, spec, oldRaw)
				if err != nil {
					return err
				}
				oldVec = preparedVec
				if oldCluster == 0 {
					oldCluster = assignedCluster
				}
			}
		}
		newCluster, newVec, err := maintenancePreparedCluster(state, spec, newRaw)
		if err != nil {
			return err
		}
		state.RecordClusterMutation(oldCluster, oldVec, newCluster, newVec)
		if oldRowOK && (!newRowOK || oldRowID != newRowID || len(newRaw) == 0) {
			batchRows[oldRowID] = maintenanceBatchRow{}
		}
		if len(newRaw) > 0 {
			rowID := newRowID
			if !newRowOK {
				rowID = oldRowID
			}
			if rowID != 0 {
				batchRows[rowID] = maintenanceBatchRow{cluster: newCluster, vec: newVec, live: newCluster > 0}
			}
		}
	}
	return nil
}

type maintenanceBatchRow struct {
	cluster int64
	vec     []float32
	live    bool
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
	vecEncoding, encodedVec, err := encodeOverlayVectorForJournal(state, spec, clusterID, prepared, epoch)
	if err != nil {
		return vecindex.OverlayMutation{}, fmt.Errorf("vector local changes: encode overlay rowid %d: %w", rowID, err)
	}
	return vecindex.OverlayMutation{
		Kind:        kind,
		Epoch:       epoch,
		Sequence:    sequence,
		ClusterID:   clusterID,
		RowID:       rowID,
		VecEncoding: vecEncoding,
		Vec:         encodedVec,
	}, nil
}

func encodeOverlayVectorForJournal(state *vecindex.IndexState, spec vecindex.IVFSpec, clusterID int64, prepared []byte, epoch uint64) (vecindex.OverlayVecEncoding, []byte, error) {
	if epoch == 0 || clusterID <= 0 || state == nil || state.ProbeState() == nil {
		return vecindex.OverlayPreparedF32, prepared, nil
	}
	centroid, err := state.ProbeState().GetReadOnly(uint32(clusterID - 1))
	if err != nil {
		return 0, nil, err
	}
	encoded, err := quantize.EncodeResidualInt8(spec.InternalMetric(), metric.BytesToFloat32(prepared), centroid, vecindex.MemberResidualBlockSize)
	if err != nil {
		return 0, nil, err
	}
	return vecindex.OverlayResidualInt8, encoded, nil
}

func buildDeleteMutation(
	state *vecindex.IndexState,
	epoch uint64,
	rowID int64,
	sequence uint64,
	force bool,
) (vecindex.OverlayMutation, bool, error) {
	if rowID == 0 {
		return vecindex.OverlayMutation{}, false, nil
	}
	if !force && !stableRowExists(state, rowID) {
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
	if value, handled, err := decodeCDCMsgpackBytes(raw); handled || err != nil {
		return value, true, err
	}
	var value any
	if err := encoding.Unmarshal(raw, &value); err != nil {
		return nil, false, err
	}
	switch v := value.(type) {
	case nil:
		return nil, true, nil
	case []byte:
		return v, true, nil
	case string:
		return []byte(v), true, nil
	default:
		return nil, false, fmt.Errorf("unexpected vector value type %T", value)
	}
}

func decodeCDCMsgpackBytes(raw []byte) ([]byte, bool, error) {
	if len(raw) == 0 {
		return nil, false, nil
	}
	switch raw[0] {
	case 0xc0:
		return nil, true, nil
	case 0xc4:
		if len(raw) < 2 {
			return nil, true, fmt.Errorf("malformed msgpack bin8")
		}
		n := int(raw[1])
		if len(raw) != 2+n {
			return nil, true, fmt.Errorf("malformed msgpack bin8 length")
		}
		return raw[2:], true, nil
	case 0xc5:
		if len(raw) < 3 {
			return nil, true, fmt.Errorf("malformed msgpack bin16")
		}
		n := int(stdbinary.BigEndian.Uint16(raw[1:3]))
		if len(raw) != 3+n {
			return nil, true, fmt.Errorf("malformed msgpack bin16 length")
		}
		return raw[3:], true, nil
	case 0xc6:
		if len(raw) < 5 {
			return nil, true, fmt.Errorf("malformed msgpack bin32")
		}
		n := int(stdbinary.BigEndian.Uint32(raw[1:5]))
		if len(raw) != 5+n {
			return nil, true, fmt.Errorf("malformed msgpack bin32 length")
		}
		return raw[5:], true, nil
	default:
		return nil, false, nil
	}
}
