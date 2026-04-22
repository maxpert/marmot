package vecindex

import "sync"

type MaintenanceState struct {
	mu                       sync.RWMutex
	ClusterRowCounts         []uint64
	ClusterVectorSums        [][]float32
	PendingClusterRowDelta   []int64
	PendingClusterVectorSums [][]float32
	RowsModifiedSinceRebuild uint64
	LastRebuildRowCount      uint64
	ConsecutiveSkewCycles    uint32
}

func (m *MaintenanceState) Clone() *MaintenanceState {
	if m == nil {
		return &MaintenanceState{}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	next := &MaintenanceState{
		RowsModifiedSinceRebuild: m.RowsModifiedSinceRebuild,
		LastRebuildRowCount:      m.LastRebuildRowCount,
		ConsecutiveSkewCycles:    m.ConsecutiveSkewCycles,
	}
	if len(m.ClusterRowCounts) > 0 {
		next.ClusterRowCounts = append([]uint64(nil), m.ClusterRowCounts...)
	}
	if len(m.ClusterVectorSums) > 0 {
		next.ClusterVectorSums = cloneVectorMatrix(m.ClusterVectorSums)
	}
	if len(m.PendingClusterRowDelta) > 0 {
		next.PendingClusterRowDelta = append([]int64(nil), m.PendingClusterRowDelta...)
	}
	if len(m.PendingClusterVectorSums) > 0 {
		next.PendingClusterVectorSums = cloneVectorMatrix(m.PendingClusterVectorSums)
	}
	return next
}

func (m *MaintenanceState) RowCount() uint64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total uint64
	for clusterID, count := range m.ClusterRowCounts {
		if clusterID == 0 {
			continue
		}
		total += count
	}
	return total
}

func (m *MaintenanceState) ensureCluster(clusterID, dim int) {
	if clusterID < 0 {
		return
	}
	if len(m.ClusterRowCounts) <= clusterID {
		need := clusterID + 1
		counts := make([]uint64, need)
		copy(counts, m.ClusterRowCounts)
		m.ClusterRowCounts = counts
	}
	if len(m.ClusterVectorSums) <= clusterID {
		need := clusterID + 1
		sums := make([][]float32, need)
		copy(sums, m.ClusterVectorSums)
		m.ClusterVectorSums = sums
	}
	if len(m.PendingClusterRowDelta) <= clusterID {
		need := clusterID + 1
		delta := make([]int64, need)
		copy(delta, m.PendingClusterRowDelta)
		m.PendingClusterRowDelta = delta
	}
	if len(m.PendingClusterVectorSums) <= clusterID {
		need := clusterID + 1
		sums := make([][]float32, need)
		copy(sums, m.PendingClusterVectorSums)
		m.PendingClusterVectorSums = sums
	}
	if clusterID >= len(m.ClusterVectorSums) {
		return
	}
	if dim <= 0 {
		return
	}
	if m.ClusterVectorSums[clusterID] == nil {
		m.ClusterVectorSums[clusterID] = make([]float32, dim)
	}
	if m.PendingClusterVectorSums[clusterID] == nil {
		m.PendingClusterVectorSums[clusterID] = make([]float32, dim)
	}
}

func (m *MaintenanceState) RecordClusterMutation(oldCluster int64, oldVec []float32, newCluster int64, newVec []float32) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if oldCluster > 0 {
		m.recordSingleClusterMutation(oldCluster, oldVec, -1)
	}
	if newCluster > 0 {
		m.recordSingleClusterMutation(newCluster, newVec, 1)
	}
}

func (m *MaintenanceState) recordSingleClusterMutation(clusterID int64, vec []float32, rowDelta int64) {
	if clusterID <= 0 {
		return
	}
	idx := int(clusterID)
	m.ensureCluster(idx, len(vec))
	m.PendingClusterRowDelta[idx] += rowDelta
	if len(vec) == 0 {
		return
	}
	scale := float32(1)
	if rowDelta < 0 {
		scale = -1
	}
	sum := m.PendingClusterVectorSums[idx]
	for i := range vec {
		sum[i] += scale * vec[i]
	}
}

func (m *MaintenanceState) LiveClusterRowCounts() []uint64 {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	live := append([]uint64(nil), m.ClusterRowCounts...)
	if len(m.PendingClusterRowDelta) > len(live) {
		ext := make([]uint64, len(m.PendingClusterRowDelta))
		copy(ext, live)
		live = ext
	}
	for clusterID := 1; clusterID < len(m.PendingClusterRowDelta); clusterID++ {
		delta := m.PendingClusterRowDelta[clusterID]
		if delta == 0 {
			continue
		}
		if delta < 0 {
			sub := uint64(-delta)
			if sub >= live[clusterID] {
				live[clusterID] = 0
			} else {
				live[clusterID] -= sub
			}
			continue
		}
		live[clusterID] += uint64(delta)
	}
	return live
}

func (m *MaintenanceState) LiveClusterVectorSums() [][]float32 {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	live := cloneVectorMatrix(m.ClusterVectorSums)
	if len(m.PendingClusterVectorSums) > len(live) {
		ext := make([][]float32, len(m.PendingClusterVectorSums))
		copy(ext, live)
		live = ext
	}
	for clusterID := 1; clusterID < len(m.PendingClusterVectorSums); clusterID++ {
		delta := m.PendingClusterVectorSums[clusterID]
		if len(delta) == 0 {
			continue
		}
		if live[clusterID] == nil {
			live[clusterID] = make([]float32, len(delta))
		}
		for i := range delta {
			live[clusterID][i] += delta[i]
		}
	}
	return live
}

func (m *MaintenanceState) LiveCentroids(base [][]float32) [][]float32 {
	if m == nil {
		return cloneVectorMatrix(base)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	centroids := cloneVectorMatrix(base)
	counts := append([]uint64(nil), m.ClusterRowCounts...)
	if len(m.PendingClusterRowDelta) > len(counts) {
		ext := make([]uint64, len(m.PendingClusterRowDelta))
		copy(ext, counts)
		counts = ext
	}
	for clusterID := 1; clusterID < len(m.PendingClusterRowDelta); clusterID++ {
		delta := m.PendingClusterRowDelta[clusterID]
		if delta == 0 {
			continue
		}
		if delta < 0 {
			sub := uint64(-delta)
			if sub >= counts[clusterID] {
				counts[clusterID] = 0
			} else {
				counts[clusterID] -= sub
			}
		} else {
			counts[clusterID] += uint64(delta)
		}
	}
	sums := cloneVectorMatrix(m.ClusterVectorSums)
	if len(m.PendingClusterVectorSums) > len(sums) {
		ext := make([][]float32, len(m.PendingClusterVectorSums))
		copy(ext, sums)
		sums = ext
	}
	for clusterID := 1; clusterID < len(m.PendingClusterVectorSums); clusterID++ {
		delta := m.PendingClusterVectorSums[clusterID]
		if len(delta) == 0 {
			continue
		}
		if sums[clusterID] == nil {
			sums[clusterID] = make([]float32, len(delta))
		}
		for i := range delta {
			sums[clusterID][i] += delta[i]
		}
	}
	maxCluster := len(counts) - 1
	if len(sums)-1 > maxCluster {
		maxCluster = len(sums) - 1
	}
	if maxCluster < 0 {
		return centroids
	}
	if len(centroids) < maxCluster {
		ext := make([][]float32, maxCluster)
		copy(ext, centroids)
		centroids = ext
	}
	for clusterID := 1; clusterID <= maxCluster; clusterID++ {
		if clusterID >= len(counts) || counts[clusterID] == 0 {
			continue
		}
		if clusterID >= len(sums) || len(sums[clusterID]) == 0 {
			continue
		}
		dim := len(sums[clusterID])
		if centroids[clusterID-1] == nil {
			centroids[clusterID-1] = make([]float32, dim)
		}
		inv := 1 / float32(counts[clusterID])
		for i := 0; i < dim; i++ {
			centroids[clusterID-1][i] = sums[clusterID][i] * inv
		}
	}
	return centroids
}

func (m *MaintenanceState) RecordRowsModified(delta uint64) {
	if m == nil || delta == 0 {
		return
	}
	m.mu.Lock()
	m.RowsModifiedSinceRebuild += delta
	m.mu.Unlock()
}

func (m *MaintenanceState) ResetPending() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.PendingClusterRowDelta {
		m.PendingClusterRowDelta[i] = 0
	}
	for i := range m.PendingClusterVectorSums {
		if m.PendingClusterVectorSums[i] != nil {
			clear(m.PendingClusterVectorSums[i])
		}
	}
}

func (m *MaintenanceState) SetConsecutiveSkewCycles(v uint32) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.ConsecutiveSkewCycles = v
	m.mu.Unlock()
}

func (m *MaintenanceState) Stats() (rowsModified uint64, lastRebuild uint64, skewCycles uint32) {
	if m == nil {
		return 0, 0, 0
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.RowsModifiedSinceRebuild, m.LastRebuildRowCount, m.ConsecutiveSkewCycles
}

func cloneVectorMatrix(src [][]float32) [][]float32 {
	if len(src) == 0 {
		return nil
	}
	out := make([][]float32, len(src))
	for i := range src {
		if len(src[i]) == 0 {
			continue
		}
		out[i] = append([]float32(nil), src[i]...)
	}
	return out
}

func cloneVectorSums(src [][]float32) [][]float32 {
	return cloneVectorMatrix(src)
}
