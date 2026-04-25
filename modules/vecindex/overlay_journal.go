package vecindex

import (
	"bufio"
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	overlayJournalMagic       = "MVTOJ001"
	overlayJournalVersion     = uint32(2)
	overlayJournalHeaderSize  = 28
	overlayJournalRecordFloor = 1 + 8 + 8 + 8 + 8 + 8 + 4
	overlayVecCacheBytes      = 64 << 20
)

var overlayJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

type overlayVecRef struct {
	offset int64
	length int
	inline []byte
}

func inlineOverlayVecRef(vec []byte) overlayVecRef {
	if len(vec) == 0 {
		return overlayVecRef{}
	}
	return overlayVecRef{length: len(vec), inline: append([]byte(nil), vec...)}
}

func journalOverlayVecRef(offset int64, length int) overlayVecRef {
	if length <= 0 {
		return overlayVecRef{}
	}
	return overlayVecRef{offset: offset, length: length}
}

type overlayVecKey struct {
	sequence uint64
	rowID    int64
}

type overlayVecReader struct {
	file  *os.File
	cache *overlayVecCache
}

func newOverlayVecReader(file *os.File) *overlayVecReader {
	if file == nil {
		return nil
	}
	return &overlayVecReader{file: file, cache: newOverlayVecCache(overlayVecCacheBytes)}
}

func (r *overlayVecReader) read(ref overlayVecRef, key overlayVecKey) ([]byte, error) {
	if len(ref.inline) > 0 {
		return ref.inline, nil
	}
	if ref.length == 0 {
		return nil, nil
	}
	if r == nil || r.file == nil || ref.offset <= 0 {
		return nil, fmt.Errorf("vecindex: overlay vector ref is not readable")
	}
	if r.cache != nil {
		if vec, ok := r.cache.Get(key); ok {
			return vec, nil
		}
	}
	vec := make([]byte, ref.length)
	if _, err := r.file.ReadAt(vec, ref.offset); err != nil {
		return nil, err
	}
	if r.cache != nil {
		r.cache.Put(key, vec)
	}
	return vec, nil
}

type overlayVecCache struct {
	mu       sync.Mutex
	maxBytes int64
	bytes    int64
	ll       *list.List
	items    map[overlayVecKey]*list.Element
}

type overlayVecCacheEntry struct {
	key overlayVecKey
	vec []byte
}

func newOverlayVecCache(maxBytes int64) *overlayVecCache {
	if maxBytes <= 0 {
		return nil
	}
	return &overlayVecCache{
		maxBytes: maxBytes,
		ll:       list.New(),
		items:    make(map[overlayVecKey]*list.Element),
	}
}

func (c *overlayVecCache) Get(key overlayVecKey) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.ll.MoveToFront(elem)
	return elem.Value.(*overlayVecCacheEntry).vec, true
}

func (c *overlayVecCache) Put(key overlayVecKey, vec []byte) {
	if c == nil || len(vec) == 0 || int64(len(vec)) > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*overlayVecCacheEntry)
		c.bytes += int64(len(vec) - len(entry.vec))
		entry.vec = vec
		c.ll.MoveToFront(elem)
	} else {
		entry := &overlayVecCacheEntry{key: key, vec: vec}
		c.items[key] = c.ll.PushFront(entry)
		c.bytes += int64(len(vec))
	}
	for c.bytes > c.maxBytes {
		elem := c.ll.Back()
		if elem == nil {
			break
		}
		entry := elem.Value.(*overlayVecCacheEntry)
		delete(c.items, entry.key)
		c.bytes -= int64(len(entry.vec))
		c.ll.Remove(elem)
	}
}

type OverlayMutationKind uint8

const (
	OverlayMutationUpsert  OverlayMutationKind = 1
	OverlayMutationReplace OverlayMutationKind = 2
	OverlayMutationDelete  OverlayMutationKind = 3
)

// OverlayMutation is one committed local overlay change.
//
// Sequence is a caller-provided monotonically increasing local watermark. Epoch
// tracks the centroid generation the assignment was computed against.
type OverlayMutation struct {
	Kind              OverlayMutationKind
	Epoch             uint64
	Sequence          uint64
	ClusterID         int64
	RowID             int64
	AppliedAtUnixNano int64
	Vec               []byte
}

// OverlaySnapshot is an immutable in-memory view of the local overlay state.
// It mirrors the serving overlay semantics used during the vector-index cutover:
// replacement/deletes tombstone the stable view while upserts only contribute
// overlay rows.
type OverlaySnapshot struct {
	epoch        uint64
	lastSequence uint64
	reader       *overlayVecReader
	byCluster    map[int64]map[int64]overlayRow
	rowCluster   map[int64]int64
	tombstones   map[int64]overlayTombstone
}

type overlayRow struct {
	sequence          uint64
	appliedAtUnixNano int64
	vec               overlayVecRef
}

type overlayTombstone struct {
	sequence          uint64
	appliedAtUnixNano int64
}

type overlayMutationRef struct {
	kind              OverlayMutationKind
	sequence          uint64
	clusterID         int64
	rowID             int64
	appliedAtUnixNano int64
	vec               overlayVecRef
}

func newOverlaySnapshot(epoch, lastSequence uint64) *OverlaySnapshot {
	return newOverlaySnapshotWithReader(epoch, lastSequence, nil)
}

func newOverlaySnapshotWithReader(epoch, lastSequence uint64, reader *overlayVecReader) *OverlaySnapshot {
	return &OverlaySnapshot{
		epoch:        epoch,
		lastSequence: lastSequence,
		reader:       reader,
		byCluster:    make(map[int64]map[int64]overlayRow),
		rowCluster:   make(map[int64]int64),
		tombstones:   make(map[int64]overlayTombstone),
	}
}

// Epoch returns the centroid generation the snapshot is tied to.
func (s *OverlaySnapshot) Epoch() uint64 {
	if s == nil {
		return 0
	}
	return s.epoch
}

// LastSequence returns the highest applied local journal sequence.
func (s *OverlaySnapshot) LastSequence() uint64 {
	if s == nil {
		return 0
	}
	return s.lastSequence
}

// Len returns the number of overlay rows currently present.
func (s *OverlaySnapshot) Len() int {
	if s == nil {
		return 0
	}
	return len(s.rowCluster)
}

// HasTombstone reports whether rowID masks a stable-row copy.
func (s *OverlaySnapshot) HasTombstone(rowID int64) bool {
	if s == nil {
		return false
	}
	_, ok := s.tombstones[rowID]
	return ok
}

func (s *OverlaySnapshot) HasTombstoneAfter(rowID int64, minSequence uint64) bool {
	if s == nil {
		return false
	}
	tombstone, ok := s.tombstones[rowID]
	return ok && tombstone.sequence > minSequence
}

// RowCluster returns the overlay cluster for rowID, if present.
func (s *OverlaySnapshot) RowCluster(rowID int64) (int64, bool) {
	if s == nil {
		return 0, false
	}
	clusterID, ok := s.rowCluster[rowID]
	return clusterID, ok
}

func (s *OverlaySnapshot) RowClusterAfter(rowID int64, minSequence uint64) (int64, bool) {
	if s == nil {
		return 0, false
	}
	clusterID, ok := s.rowCluster[rowID]
	if !ok {
		return 0, false
	}
	row, ok := s.byCluster[clusterID][rowID]
	if !ok || row.sequence <= minSequence {
		return 0, false
	}
	return clusterID, true
}

// VisitCluster visits every overlay row in clusterID.
func (s *OverlaySnapshot) VisitCluster(clusterID int64, visit func(rowID int64, vec []byte) bool) {
	s.VisitClusterAfter(clusterID, 0, visit)
}

func (s *OverlaySnapshot) VisitClusterAfter(clusterID int64, minSequence uint64, visit func(rowID int64, vec []byte) bool) {
	if s == nil || visit == nil {
		return
	}
	for rowID, row := range s.byCluster[clusterID] {
		if row.sequence <= minSequence {
			continue
		}
		vec, err := s.readRowVec(rowID, row)
		if err != nil {
			return
		}
		if !visit(rowID, vec) {
			return
		}
	}
}

func (s *OverlaySnapshot) VisitClusterRange(clusterID int64, minSequence uint64, maxSequence uint64, visit func(rowID int64, vec []byte) bool) {
	if s == nil || visit == nil {
		return
	}
	for rowID, row := range s.byCluster[clusterID] {
		if row.sequence <= minSequence {
			continue
		}
		if maxSequence > 0 && row.sequence > maxSequence {
			continue
		}
		vec, err := s.readRowVec(rowID, row)
		if err != nil {
			return
		}
		if !visit(rowID, vec) {
			return
		}
	}
}

// VisitTombstones visits every tombstoned rowid.
func (s *OverlaySnapshot) VisitTombstones(visit func(rowID int64) bool) {
	s.VisitTombstonesAfter(0, visit)
}

func (s *OverlaySnapshot) VisitTombstonesAfter(minSequence uint64, visit func(rowID int64) bool) {
	if s == nil || visit == nil {
		return
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if !visit(rowID) {
			return
		}
	}
}

func (s *OverlaySnapshot) VisitAllAfter(minSequence uint64, visit func(clusterID, rowID int64, vec []byte) bool) {
	if s == nil || visit == nil {
		return
	}
	for clusterID, rows := range s.byCluster {
		for rowID, row := range rows {
			if row.sequence <= minSequence {
				continue
			}
			vec, err := s.readRowVec(rowID, row)
			if err != nil {
				return
			}
			if !visit(clusterID, rowID, vec) {
				return
			}
		}
	}
}

func (s *OverlaySnapshot) BacklogStats(minSequence uint64) (rows int, bytes int64, oldestUnixNano int64) {
	if s == nil {
		return 0, 0, 0
	}
	for _, clusterRows := range s.byCluster {
		for _, row := range clusterRows {
			if row.sequence <= minSequence {
				continue
			}
			rows++
			bytes += int64(row.vec.length)
			if oldestUnixNano == 0 || row.appliedAtUnixNano < oldestUnixNano {
				oldestUnixNano = row.appliedAtUnixNano
			}
		}
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if clusterID, ok := s.rowCluster[rowID]; ok {
			if row, ok := s.byCluster[clusterID][rowID]; ok && row.sequence == tombstone.sequence {
				continue
			}
		}
		rows++
		if oldestUnixNano == 0 || tombstone.appliedAtUnixNano < oldestUnixNano {
			oldestUnixNano = tombstone.appliedAtUnixNano
		}
	}
	return rows, bytes, oldestUnixNano
}

func (s *OverlaySnapshot) NewestUnixNanoAfter(minSequence uint64) int64 {
	if s == nil {
		return 0
	}
	var newest int64
	for _, clusterRows := range s.byCluster {
		for _, row := range clusterRows {
			if row.sequence <= minSequence {
				continue
			}
			if row.appliedAtUnixNano > newest {
				newest = row.appliedAtUnixNano
			}
		}
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if clusterID, ok := s.rowCluster[rowID]; ok {
			if row, ok := s.byCluster[clusterID][rowID]; ok && row.sequence == tombstone.sequence {
				continue
			}
		}
		if tombstone.appliedAtUnixNano > newest {
			newest = tombstone.appliedAtUnixNano
		}
	}
	return newest
}

func (s *OverlaySnapshot) MutationsAfter(minSequence uint64) []OverlayMutation {
	if s == nil {
		return nil
	}
	mutations := make([]OverlayMutation, 0, len(s.rowCluster)+len(s.tombstones))
	for clusterID, rows := range s.byCluster {
		for rowID, row := range rows {
			if row.sequence <= minSequence {
				continue
			}
			vec, err := s.readRowVec(rowID, row)
			if err != nil {
				continue
			}
			kind := OverlayMutationUpsert
			if tombstone, ok := s.tombstones[rowID]; ok && tombstone.sequence == row.sequence {
				kind = OverlayMutationReplace
			}
			mutations = append(mutations, OverlayMutation{
				Kind:              kind,
				Epoch:             s.epoch,
				Sequence:          row.sequence,
				ClusterID:         clusterID,
				RowID:             rowID,
				AppliedAtUnixNano: row.appliedAtUnixNano,
				Vec:               append([]byte(nil), vec...),
			})
		}
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if _, ok := s.rowCluster[rowID]; ok {
			continue
		}
		mutations = append(mutations, OverlayMutation{
			Kind:              OverlayMutationDelete,
			Epoch:             s.epoch,
			Sequence:          tombstone.sequence,
			RowID:             rowID,
			AppliedAtUnixNano: tombstone.appliedAtUnixNano,
		})
	}
	slices.SortFunc(mutations, func(a, b OverlayMutation) int {
		switch {
		case a.Sequence < b.Sequence:
			return -1
		case a.Sequence > b.Sequence:
			return 1
		case a.RowID < b.RowID:
			return -1
		case a.RowID > b.RowID:
			return 1
		default:
			return 0
		}
	})
	return mutations
}

func (s *OverlaySnapshot) VisitMutationsAfter(minSequence uint64, visit func(OverlayMutation) bool) {
	if s == nil || visit == nil {
		return
	}
	for _, ref := range s.mutationRefsAfter(minSequence) {
		vec, err := s.readVec(ref.vec, ref.sequence, ref.rowID)
		if err != nil {
			return
		}
		if !visit(OverlayMutation{
			Kind:              ref.kind,
			Epoch:             s.epoch,
			Sequence:          ref.sequence,
			ClusterID:         ref.clusterID,
			RowID:             ref.rowID,
			AppliedAtUnixNano: ref.appliedAtUnixNano,
			Vec:               vec,
		}) {
			return
		}
	}
}

func (s *OverlaySnapshot) VisitMutationHeadersAfter(minSequence uint64, visit func(OverlayMutation) bool) {
	if s == nil || visit == nil {
		return
	}
	for _, ref := range s.mutationRefsAfter(minSequence) {
		if !visit(OverlayMutation{
			Kind:              ref.kind,
			Epoch:             s.epoch,
			Sequence:          ref.sequence,
			ClusterID:         ref.clusterID,
			RowID:             ref.rowID,
			AppliedAtUnixNano: ref.appliedAtUnixNano,
		}) {
			return
		}
	}
}

// VisitMutationHeadersAfterUnordered visits mutation headers without building a
// sorted intermediate slice. Use it for maintenance accounting where sequence
// order is not required.
func (s *OverlaySnapshot) VisitMutationHeadersAfterUnordered(minSequence uint64, visit func(OverlayMutation) bool) {
	if s == nil || visit == nil {
		return
	}
	for clusterID, rows := range s.byCluster {
		for rowID, row := range rows {
			if row.sequence <= minSequence {
				continue
			}
			kind := OverlayMutationUpsert
			if tombstone, ok := s.tombstones[rowID]; ok && tombstone.sequence == row.sequence {
				kind = OverlayMutationReplace
			}
			if !visit(OverlayMutation{
				Kind:              kind,
				Epoch:             s.epoch,
				Sequence:          row.sequence,
				ClusterID:         clusterID,
				RowID:             rowID,
				AppliedAtUnixNano: row.appliedAtUnixNano,
			}) {
				return
			}
		}
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if _, ok := s.rowCluster[rowID]; ok {
			continue
		}
		if !visit(OverlayMutation{
			Kind:              OverlayMutationDelete,
			Epoch:             s.epoch,
			Sequence:          tombstone.sequence,
			RowID:             rowID,
			AppliedAtUnixNano: tombstone.appliedAtUnixNano,
		}) {
			return
		}
	}
}

func (s *OverlaySnapshot) mutationRefsAfter(minSequence uint64) []overlayMutationRef {
	if s == nil {
		return nil
	}
	refs := make([]overlayMutationRef, 0, len(s.rowCluster)+len(s.tombstones))
	for clusterID, rows := range s.byCluster {
		for rowID, row := range rows {
			if row.sequence <= minSequence {
				continue
			}
			kind := OverlayMutationUpsert
			if tombstone, ok := s.tombstones[rowID]; ok && tombstone.sequence == row.sequence {
				kind = OverlayMutationReplace
			}
			refs = append(refs, overlayMutationRef{
				kind:              kind,
				sequence:          row.sequence,
				clusterID:         clusterID,
				rowID:             rowID,
				appliedAtUnixNano: row.appliedAtUnixNano,
				vec:               row.vec,
			})
		}
	}
	for rowID, tombstone := range s.tombstones {
		if tombstone.sequence <= minSequence {
			continue
		}
		if _, ok := s.rowCluster[rowID]; ok {
			continue
		}
		refs = append(refs, overlayMutationRef{
			kind:              OverlayMutationDelete,
			sequence:          tombstone.sequence,
			rowID:             rowID,
			appliedAtUnixNano: tombstone.appliedAtUnixNano,
		})
	}
	slices.SortFunc(refs, func(a, b overlayMutationRef) int {
		switch {
		case a.sequence < b.sequence:
			return -1
		case a.sequence > b.sequence:
			return 1
		case a.rowID < b.rowID:
			return -1
		case a.rowID > b.rowID:
			return 1
		default:
			return 0
		}
	})
	return refs
}

// ReadVec returns the current overlay vector bytes for rowID.
func (s *OverlaySnapshot) ReadVec(rowID int64) ([]byte, error) {
	if s == nil {
		return nil, fmt.Errorf("vecindex: overlay snapshot is nil")
	}
	clusterID, ok := s.rowCluster[rowID]
	if !ok {
		return nil, fmt.Errorf("vecindex: overlay rowid %d not found", rowID)
	}
	row, ok := s.byCluster[clusterID][rowID]
	if !ok {
		return nil, fmt.Errorf("vecindex: overlay rowid %d missing cluster row", rowID)
	}
	return s.readRowVec(rowID, row)
}

func (s *OverlaySnapshot) readRowVec(rowID int64, row overlayRow) ([]byte, error) {
	return s.readVec(row.vec, row.sequence, rowID)
}

func (s *OverlaySnapshot) readVec(ref overlayVecRef, sequence uint64, rowID int64) ([]byte, error) {
	if ref.length == 0 {
		return nil, nil
	}
	if len(ref.inline) > 0 {
		return ref.inline, nil
	}
	if s == nil || s.reader == nil {
		return nil, fmt.Errorf("vecindex: overlay vector rowid %d has no reader", rowID)
	}
	return s.reader.read(ref, overlayVecKey{sequence: sequence, rowID: rowID})
}

func (s *OverlaySnapshot) clone() *OverlaySnapshot {
	if s == nil {
		return newOverlaySnapshot(0, 0)
	}
	next := &OverlaySnapshot{
		epoch:        s.epoch,
		lastSequence: s.lastSequence,
		reader:       s.reader,
		byCluster:    make(map[int64]map[int64]overlayRow, len(s.byCluster)),
		rowCluster:   make(map[int64]int64, len(s.rowCluster)),
		tombstones:   make(map[int64]overlayTombstone, len(s.tombstones)),
	}
	for clusterID, rows := range s.byCluster {
		copied := make(map[int64]overlayRow, len(rows))
		for rowID, row := range rows {
			copied[rowID] = overlayRow{
				sequence:          row.sequence,
				appliedAtUnixNano: row.appliedAtUnixNano,
				vec:               row.vec,
			}
		}
		next.byCluster[clusterID] = copied
	}
	for rowID, clusterID := range s.rowCluster {
		next.rowCluster[rowID] = clusterID
	}
	for rowID, tombstone := range s.tombstones {
		next.tombstones[rowID] = tombstone
	}
	return next
}

func (s *OverlaySnapshot) applyBatch(mutations []OverlayMutation) (*OverlaySnapshot, error) {
	if len(mutations) == 0 {
		return s, nil
	}
	refs := make([]overlayVecRef, len(mutations))
	for i, mutation := range mutations {
		refs[i] = inlineOverlayVecRef(mutation.Vec)
	}
	return s.applyBatchRefs(mutations, refs, nil)
}

func (s *OverlaySnapshot) applyBatchRefs(mutations []OverlayMutation, refs []overlayVecRef, reader *overlayVecReader) (*OverlaySnapshot, error) {
	if len(mutations) == 0 {
		return s, nil
	}
	if len(refs) != len(mutations) {
		return nil, fmt.Errorf("overlay mutation refs length mismatch")
	}

	current := s
	if current == nil {
		current = newOverlaySnapshotWithReader(0, 0, reader)
	}
	next := current.clone()
	if reader != nil {
		next.reader = reader
	}
	for i, mutation := range mutations {
		if err := validateOverlayMutationRef(mutation, refs[i]); err != nil {
			return nil, fmt.Errorf("overlay mutation %d: %w", i, err)
		}
		if mutation.Epoch < next.epoch {
			return nil, fmt.Errorf("overlay mutation %d: epoch %d regresses from %d", i, mutation.Epoch, next.epoch)
		}
		if mutation.Epoch > next.epoch {
			next = newOverlaySnapshotWithReader(mutation.Epoch, 0, reader)
		}
		if mutation.Sequence <= next.lastSequence {
			return nil, fmt.Errorf("overlay mutation %d: sequence %d must be greater than %d", i, mutation.Sequence, next.lastSequence)
		}
		if i > 0 {
			prev := mutations[i-1]
			if mutation.Epoch < prev.Epoch {
				return nil, fmt.Errorf("overlay mutation %d: epoch %d regresses from %d within batch", i, mutation.Epoch, prev.Epoch)
			}
			if mutation.Epoch == prev.Epoch && mutation.Sequence <= prev.Sequence {
				return nil, fmt.Errorf("overlay mutation %d: sequence %d must be strictly increasing", i, mutation.Sequence)
			}
		}
		next.applyMutationRef(mutation, refs[i])
		next.lastSequence = mutation.Sequence
		next.epoch = mutation.Epoch
	}
	return next, nil
}

func (s *OverlaySnapshot) applyMutation(mutation OverlayMutation) {
	s.applyMutationRef(mutation, inlineOverlayVecRef(mutation.Vec))
}

func (s *OverlaySnapshot) applyMutationRef(mutation OverlayMutation, ref overlayVecRef) {
	switch mutation.Kind {
	case OverlayMutationUpsert:
		s.removeRow(mutation.RowID)
		delete(s.tombstones, mutation.RowID)
		s.upsertRow(mutation.ClusterID, mutation.RowID, mutation.Sequence, mutation.AppliedAtUnixNano, ref)
	case OverlayMutationReplace:
		s.removeRow(mutation.RowID)
		s.tombstones[mutation.RowID] = overlayTombstone{
			sequence:          mutation.Sequence,
			appliedAtUnixNano: mutation.AppliedAtUnixNano,
		}
		s.upsertRow(mutation.ClusterID, mutation.RowID, mutation.Sequence, mutation.AppliedAtUnixNano, ref)
	case OverlayMutationDelete:
		s.removeRow(mutation.RowID)
		s.tombstones[mutation.RowID] = overlayTombstone{
			sequence:          mutation.Sequence,
			appliedAtUnixNano: mutation.AppliedAtUnixNano,
		}
	}
}

func (s *OverlaySnapshot) removeRow(rowID int64) {
	clusterID, ok := s.rowCluster[rowID]
	if !ok {
		return
	}
	if rows := s.byCluster[clusterID]; rows != nil {
		delete(rows, rowID)
		if len(rows) == 0 {
			delete(s.byCluster, clusterID)
		}
	}
	delete(s.rowCluster, rowID)
}

func (s *OverlaySnapshot) upsertRow(clusterID, rowID int64, sequence uint64, appliedAtUnixNano int64, vec overlayVecRef) {
	rows := s.byCluster[clusterID]
	if rows == nil {
		rows = make(map[int64]overlayRow)
		s.byCluster[clusterID] = rows
	}
	rows[rowID] = overlayRow{
		sequence:          sequence,
		appliedAtUnixNano: appliedAtUnixNano,
		vec:               vec,
	}
	s.rowCluster[rowID] = clusterID
}

func validateOverlayMutation(mutation OverlayMutation) error {
	return validateOverlayMutationRef(mutation, inlineOverlayVecRef(mutation.Vec))
}

func validateOverlayMutationRef(mutation OverlayMutation, ref overlayVecRef) error {
	if mutation.Sequence == 0 {
		return fmt.Errorf("sequence must be > 0")
	}
	if mutation.RowID == 0 {
		return fmt.Errorf("rowid must be non-zero")
	}
	if mutation.ClusterID < 0 {
		return fmt.Errorf("cluster_id %d must be >= 0", mutation.ClusterID)
	}
	switch mutation.Kind {
	case OverlayMutationUpsert, OverlayMutationReplace:
		if ref.length == 0 {
			return fmt.Errorf("vec must be non-empty for kind %d", mutation.Kind)
		}
	case OverlayMutationDelete:
		if ref.length != 0 {
			return fmt.Errorf("delete mutation must not carry vec bytes")
		}
	default:
		return fmt.Errorf("unknown mutation kind %d", mutation.Kind)
	}
	return nil
}

// OverlayBuffer stores immutable overlay snapshots behind an atomic pointer so
// readers can take stable snapshots without locking while writers publish
// whole-snapshot updates.
type OverlayBuffer struct {
	snapshot atomic.Pointer[OverlaySnapshot]
}

// NewOverlayBuffer returns an empty overlay buffer.
func NewOverlayBuffer() *OverlayBuffer {
	b := &OverlayBuffer{}
	b.snapshot.Store(newOverlaySnapshot(0, 0))
	return b
}

// Snapshot returns the current immutable overlay view.
func (b *OverlayBuffer) Snapshot() *OverlaySnapshot {
	if b == nil {
		return nil
	}
	return b.snapshot.Load()
}

// StoreSnapshot replaces the current overlay view.
func (b *OverlayBuffer) StoreSnapshot(snapshot *OverlaySnapshot) {
	if b == nil {
		return
	}
	if snapshot == nil {
		snapshot = newOverlaySnapshot(0, 0)
	}
	b.snapshot.Store(snapshot)
}

// ApplyBatch applies mutations copy-on-write and publishes the resulting
// snapshot atomically.
func (b *OverlayBuffer) ApplyBatch(mutations []OverlayMutation) error {
	if len(mutations) == 0 || b == nil {
		return nil
	}
	for {
		current := b.snapshot.Load()
		next, err := current.applyBatch(mutations)
		if err != nil {
			return err
		}
		if next == current || b.snapshot.CompareAndSwap(current, next) {
			return nil
		}
	}
}

// Reset clears the overlay and publishes an empty snapshot tied to epoch.
func (b *OverlayBuffer) Reset(epoch uint64) {
	if b == nil {
		return
	}
	b.snapshot.Store(newOverlaySnapshot(epoch, 0))
}

type OverlayJournal struct {
	path         string
	file         *os.File
	reader       *overlayVecReader
	mu           sync.Mutex
	currentEpoch uint64
	lastSequence uint64
}

// OpenOverlayJournal opens or creates a crash-safe append-only overlay journal.
// A truncated tail left by a torn write is discarded automatically.
func OpenOverlayJournal(path string) (*OverlayJournal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	reader := newOverlayVecReader(file)
	snapshot, validEnd, err := replayOverlayJournalFile(file, reader)
	if err != nil {
		file.Close()
		return nil, err
	}
	if err := file.Truncate(validEnd); err != nil {
		file.Close()
		return nil, err
	}
	if _, err := file.Seek(validEnd, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	return &OverlayJournal{
		path:         path,
		file:         file,
		reader:       reader,
		currentEpoch: snapshot.Epoch(),
		lastSequence: snapshot.LastSequence(),
	}, nil
}

// OpenOverlayJournalForRewrite opens or creates the journal file without
// replaying its contents. Use this only for callers that will immediately
// replace the on-disk contents via Rewrite/Reset.
func OpenOverlayJournalForRewrite(path string) (*OverlayJournal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &OverlayJournal{
		path:   path,
		file:   file,
		reader: newOverlayVecReader(file),
	}, nil
}

// Replay rebuilds the overlay snapshot from the journal contents.
func (j *OverlayJournal) Replay() (*OverlaySnapshot, error) {
	if j == nil || j.file == nil {
		return newOverlaySnapshot(0, 0), nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.reader = newOverlayVecReader(j.file)
	snapshot, validEnd, err := replayOverlayJournalFile(j.file, j.reader)
	if err != nil {
		return nil, err
	}
	if err := j.file.Truncate(validEnd); err != nil {
		return nil, err
	}
	if _, err := j.file.Seek(validEnd, io.SeekStart); err != nil {
		return nil, err
	}
	j.currentEpoch = snapshot.Epoch()
	j.lastSequence = snapshot.LastSequence()
	return snapshot, nil
}

// AppendBatch appends committed overlay mutations atomically and fsyncs once
// per batch so replay after a crash sees either the full batch or none of the
// torn tail.
func (j *OverlayJournal) AppendBatch(mutations []OverlayMutation) ([]overlayVecRef, error) {
	if j == nil {
		return nil, fmt.Errorf("vecindex: overlay journal is nil")
	}
	if len(mutations) == 0 {
		return nil, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	startOffset, err := j.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	writer := bufio.NewWriterSize(j.file, 256<<10)
	epoch, lastSequence, refs, err := writeOverlayJournalBatch(writer, startOffset, j.currentEpoch, j.lastSequence, mutations)
	if err != nil {
		return nil, err
	}
	if err := writer.Flush(); err != nil {
		return nil, err
	}
	if err := j.file.Sync(); err != nil {
		return nil, err
	}
	j.currentEpoch = epoch
	j.lastSequence = lastSequence
	return refs, nil
}

// Reset compacts the journal to an empty state for epoch.
func (j *OverlayJournal) Reset(epoch uint64) error {
	if j == nil {
		return fmt.Errorf("vecindex: overlay journal is nil")
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	tmpPath := j.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := writeOverlayJournalHeader(tmp, epoch, 0); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := j.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	file, err := os.OpenFile(j.path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Seek(overlayJournalHeaderSize, io.SeekStart); err != nil {
		file.Close()
		return err
	}
	j.file = file
	j.reader = newOverlayVecReader(file)
	j.currentEpoch = epoch
	j.lastSequence = 0
	return nil
}

// Close closes the underlying file handle.
func (j *OverlayJournal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	return j.file.Close()
}

func (j *OverlayJournal) Rewrite(epoch uint64, lastSequence uint64, mutations []OverlayMutation) ([]overlayVecRef, error) {
	if j == nil {
		return nil, fmt.Errorf("vecindex: overlay journal is nil")
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	tmpPath := j.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if err := writeOverlayJournalHeader(tmp, epoch, lastSequence); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	currentEpoch := epoch
	currentSequence := lastSequence
	var refs []overlayVecRef
	if len(mutations) > 0 {
		writer := bufio.NewWriterSize(tmp, 256<<10)
		startOffset, err := tmp.Seek(0, io.SeekCurrent)
		if err != nil {
			tmp.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}
		_, rewrittenLastSequence, rewrittenRefs, err := writeOverlayJournalBatch(writer, startOffset, epoch, 0, mutations)
		if err != nil {
			tmp.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}
		if err := writer.Flush(); err != nil {
			tmp.Close()
			_ = os.Remove(tmpPath)
			return nil, err
		}
		currentEpoch = mutations[len(mutations)-1].Epoch
		currentSequence = rewrittenLastSequence
		refs = rewrittenRefs
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := j.file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	if err := os.Rename(tmpPath, j.path); err != nil {
		_ = os.Remove(tmpPath)
		return nil, err
	}
	file, err := os.OpenFile(j.path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	end := int64(overlayJournalHeaderSize)
	if len(mutations) > 0 {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return nil, err
		}
		end = info.Size()
	}
	if _, err := file.Seek(end, io.SeekStart); err != nil {
		file.Close()
		return nil, err
	}
	j.file = file
	j.reader = newOverlayVecReader(file)
	j.currentEpoch = currentEpoch
	j.lastSequence = currentSequence
	for i := range refs {
		refs[i].inline = nil
	}
	return refs, nil
}

// JournaledOverlay couples a crash-safe local journal with an in-memory
// overlay snapshot. Callers append committed batches here and readers consume
// immutable snapshots.
type JournaledOverlay struct {
	journal *OverlayJournal
	buffer  *OverlayBuffer
}

// OpenJournaledOverlay opens path, replays the journal, and returns the live
// in-memory overlay view.
func OpenJournaledOverlay(path string) (*JournaledOverlay, error) {
	journal, err := OpenOverlayJournal(path)
	if err != nil {
		return nil, err
	}
	snapshot, err := journal.Replay()
	if err != nil {
		journal.Close()
		return nil, err
	}
	buffer := NewOverlayBuffer()
	buffer.StoreSnapshot(snapshot)
	return &JournaledOverlay{
		journal: journal,
		buffer:  buffer,
	}, nil
}

// OpenJournaledOverlayForRewrite opens the journal file without replaying the
// current overlay state. It is intended for callers that already have the
// mutations they want to persist and will immediately call Rewrite.
func OpenJournaledOverlayForRewrite(path string) (*JournaledOverlay, error) {
	journal, err := OpenOverlayJournalForRewrite(path)
	if err != nil {
		return nil, err
	}
	buffer := NewOverlayBuffer()
	buffer.StoreSnapshot(newOverlaySnapshotWithReader(0, 0, journal.reader))
	return &JournaledOverlay{
		journal: journal,
		buffer:  buffer,
	}, nil
}

// Snapshot returns the current immutable in-memory overlay view.
func (o *JournaledOverlay) Snapshot() *OverlaySnapshot {
	if o == nil || o.buffer == nil {
		return nil
	}
	return o.buffer.Snapshot()
}

// ApplyCommittedBatch validates, journals, fsyncs, and publishes a committed
// overlay batch.
func (o *JournaledOverlay) ApplyCommittedBatch(mutations []OverlayMutation) error {
	if o == nil || o.journal == nil || o.buffer == nil {
		return fmt.Errorf("vecindex: journaled overlay is not initialized")
	}
	if len(mutations) == 0 {
		return nil
	}
	refs, err := o.journal.AppendBatch(mutations)
	if err != nil {
		return err
	}
	next, err := o.buffer.Snapshot().applyBatchRefs(mutations, refs, o.journal.reader)
	if err != nil {
		return err
	}
	o.buffer.StoreSnapshot(next)
	return nil
}

// Reset clears both the journal file and the in-memory overlay view.
func (o *JournaledOverlay) Reset(epoch uint64) error {
	if o == nil || o.journal == nil || o.buffer == nil {
		return fmt.Errorf("vecindex: journaled overlay is not initialized")
	}
	if err := o.journal.Reset(epoch); err != nil {
		return err
	}
	o.buffer.StoreSnapshot(newOverlaySnapshotWithReader(epoch, 0, o.journal.reader))
	return nil
}

func (o *JournaledOverlay) CompactAfter(minSequence uint64) error {
	if o == nil || o.journal == nil || o.buffer == nil {
		return fmt.Errorf("vecindex: journaled overlay is not initialized")
	}
	current := o.buffer.Snapshot()
	if current == nil {
		return o.Reset(0)
	}
	mutations := make([]OverlayMutation, 0)
	current.VisitMutationsAfter(minSequence, func(mutation OverlayMutation) bool {
		mutations = append(mutations, mutation)
		return true
	})
	refs, err := o.journal.Rewrite(current.Epoch(), current.LastSequence(), mutations)
	if err != nil {
		return err
	}
	next := newOverlaySnapshotWithReader(current.Epoch(), current.LastSequence(), o.journal.reader)
	if len(mutations) > 0 {
		next, err = newOverlaySnapshotWithReader(current.Epoch(), minSequence, o.journal.reader).applyBatchRefs(mutations, refs, o.journal.reader)
		if err != nil {
			return err
		}
	}
	o.buffer.StoreSnapshot(next)
	return nil
}

func (o *JournaledOverlay) Rewrite(epoch uint64, minSequence uint64, mutations []OverlayMutation) error {
	if o == nil || o.journal == nil || o.buffer == nil {
		return fmt.Errorf("vecindex: journaled overlay is not initialized")
	}
	refs, err := o.journal.Rewrite(epoch, minSequence, mutations)
	if err != nil {
		return err
	}
	next := newOverlaySnapshotWithReader(epoch, minSequence, o.journal.reader)
	if len(mutations) > 0 {
		next, err = next.applyBatchRefs(mutations, refs, o.journal.reader)
		if err != nil {
			return err
		}
	}
	o.buffer.StoreSnapshot(next)
	return nil
}

// Close closes the underlying journal handle.
func (o *JournaledOverlay) Close() error {
	if o == nil || o.journal == nil {
		return nil
	}
	return o.journal.Close()
}

func writeOverlayJournalBatch(w io.Writer, startOffset int64, currentEpoch, currentSequence uint64, mutations []OverlayMutation) (uint64, uint64, []overlayVecRef, error) {
	epoch := currentEpoch
	lastSequence := currentSequence
	refs := make([]overlayVecRef, len(mutations))
	offset := startOffset
	for i, mutation := range mutations {
		if err := validateOverlayMutation(mutation); err != nil {
			return 0, 0, nil, fmt.Errorf("overlay mutation %d: %w", i, err)
		}
		if mutation.Epoch < epoch {
			return 0, 0, nil, fmt.Errorf("overlay mutation %d: epoch %d regresses from %d", i, mutation.Epoch, epoch)
		}
		if mutation.Epoch > epoch {
			epoch = mutation.Epoch
			lastSequence = 0
		}
		if mutation.Sequence <= lastSequence {
			return 0, 0, nil, fmt.Errorf("overlay mutation %d: sequence %d must be greater than %d", i, mutation.Sequence, lastSequence)
		}
		payloadLen := overlayJournalRecordFloor + len(mutation.Vec)
		if len(mutation.Vec) > 0 && offset >= 0 {
			refs[i] = journalOverlayVecRef(offset+4+overlayJournalRecordFloor, len(mutation.Vec))
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(payloadLen))
		var payloadHeader [overlayJournalRecordFloor]byte
		payloadHeader[0] = byte(mutation.Kind)
		binary.LittleEndian.PutUint64(payloadHeader[1:9], mutation.Epoch)
		binary.LittleEndian.PutUint64(payloadHeader[9:17], mutation.Sequence)
		binary.LittleEndian.PutUint64(payloadHeader[17:25], uint64(mutation.ClusterID))
		binary.LittleEndian.PutUint64(payloadHeader[25:33], uint64(mutation.RowID))
		binary.LittleEndian.PutUint64(payloadHeader[33:41], uint64(mutation.AppliedAtUnixNano))
		binary.LittleEndian.PutUint32(payloadHeader[41:45], uint32(len(mutation.Vec)))
		crc := crc32.Update(0, overlayJournalCRCTable, payloadHeader[:])
		crc = crc32.Update(crc, overlayJournalCRCTable, mutation.Vec)
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc)
		if _, err := w.Write(lenBuf[:]); err != nil {
			return 0, 0, nil, err
		}
		if _, err := w.Write(payloadHeader[:]); err != nil {
			return 0, 0, nil, err
		}
		if _, err := w.Write(mutation.Vec); err != nil {
			return 0, 0, nil, err
		}
		if _, err := w.Write(crcBuf[:]); err != nil {
			return 0, 0, nil, err
		}
		if offset >= 0 {
			offset += int64(4 + payloadLen + 4)
		}
		epoch = mutation.Epoch
		lastSequence = mutation.Sequence
	}
	return epoch, lastSequence, refs, nil
}

func replayOverlayJournalFile(file *os.File, reader *overlayVecReader) (*OverlaySnapshot, int64, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	if size == 0 {
		if err := writeOverlayJournalHeader(file, 0, 0); err != nil {
			return nil, 0, err
		}
		if err := file.Sync(); err != nil {
			return nil, 0, err
		}
		return newOverlaySnapshotWithReader(0, 0, reader), overlayJournalHeaderSize, nil
	}
	if size < overlayJournalHeaderSize {
		return nil, 0, fmt.Errorf("vecindex: overlay journal %q is too small", file.Name())
	}

	header := make([]byte, overlayJournalHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, 0, err
	}
	if string(header[:8]) != overlayJournalMagic {
		return nil, 0, fmt.Errorf("vecindex: invalid overlay journal magic %q", file.Name())
	}
	if got := binary.LittleEndian.Uint32(header[8:12]); got != overlayJournalVersion {
		return nil, 0, fmt.Errorf("vecindex: unsupported overlay journal version %d", got)
	}
	headerEpoch := binary.LittleEndian.Uint64(header[12:20])
	headerLastSequence := binary.LittleEndian.Uint64(header[20:28])

	snapshot := newOverlaySnapshotWithReader(0, 0, reader)
	if size == overlayJournalHeaderSize {
		snapshot = newOverlaySnapshotWithReader(headerEpoch, headerLastSequence, reader)
	}
	scratch := make([]byte, 256<<10)
	offset := int64(overlayJournalHeaderSize)
	for offset < size {
		recordOffset := offset
		var lenBuf [4]byte
		n, err := io.ReadFull(file, lenBuf[:])
		switch {
		case err == nil:
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			return snapshot, recordOffset, nil
		default:
			return nil, 0, err
		}
		offset += int64(n)

		recordLen := int64(binary.LittleEndian.Uint32(lenBuf[:]))
		if recordLen < overlayJournalRecordFloor {
			return nil, 0, fmt.Errorf("vecindex: invalid overlay journal record length %d", recordLen)
		}
		if offset+recordLen+4 > size {
			return snapshot, recordOffset, nil
		}

		var payloadHeader [overlayJournalRecordFloor]byte
		if _, err := io.ReadFull(file, payloadHeader[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return snapshot, recordOffset, nil
			}
			return nil, 0, err
		}
		offset += overlayJournalRecordFloor
		vecLen := int(binary.LittleEndian.Uint32(payloadHeader[41:45]))
		if vecLen != int(recordLen)-overlayJournalRecordFloor {
			return nil, 0, fmt.Errorf("vecindex: overlay journal payload length mismatch")
		}
		crc := crc32.Update(0, overlayJournalCRCTable, payloadHeader[:])
		vecOffset := offset
		remaining := vecLen
		for remaining > 0 {
			chunk := scratch
			if len(chunk) > remaining {
				chunk = chunk[:remaining]
			}
			if _, err := io.ReadFull(file, chunk); err != nil {
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return snapshot, recordOffset, nil
				}
				return nil, 0, err
			}
			crc = crc32.Update(crc, overlayJournalCRCTable, chunk)
			offset += int64(len(chunk))
			remaining -= len(chunk)
		}

		var crcBuf [4]byte
		if _, err := io.ReadFull(file, crcBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return snapshot, recordOffset, nil
			}
			return nil, 0, err
		}
		offset += 4
		if want, got := binary.LittleEndian.Uint32(crcBuf[:]), crc; want != got {
			return nil, 0, fmt.Errorf("vecindex: overlay journal checksum mismatch at offset %d", recordOffset)
		}
		mutation, err := decodeOverlayJournalRecordHeader(payloadHeader[:])
		if err != nil {
			return nil, 0, err
		}
		ref := journalOverlayVecRef(vecOffset, vecLen)
		next, err := snapshot.applyBatchRefs([]OverlayMutation{mutation}, []overlayVecRef{ref}, reader)
		if err != nil {
			return nil, 0, err
		}
		snapshot = next
	}
	return snapshot, offset, nil
}

func decodeOverlayJournalRecord(payload []byte) (OverlayMutation, error) {
	if len(payload) < overlayJournalRecordFloor {
		return OverlayMutation{}, fmt.Errorf("vecindex: overlay journal payload too small")
	}
	vecLen := int(binary.LittleEndian.Uint32(payload[41:45]))
	if vecLen != len(payload)-overlayJournalRecordFloor {
		return OverlayMutation{}, fmt.Errorf("vecindex: overlay journal payload length mismatch")
	}
	mutation := OverlayMutation{
		Kind:              OverlayMutationKind(payload[0]),
		Epoch:             binary.LittleEndian.Uint64(payload[1:9]),
		Sequence:          binary.LittleEndian.Uint64(payload[9:17]),
		ClusterID:         int64(binary.LittleEndian.Uint64(payload[17:25])),
		RowID:             int64(binary.LittleEndian.Uint64(payload[25:33])),
		AppliedAtUnixNano: int64(binary.LittleEndian.Uint64(payload[33:41])),
	}
	if vecLen > 0 {
		mutation.Vec = append([]byte(nil), payload[45:]...)
	}
	if err := validateOverlayMutation(mutation); err != nil {
		return OverlayMutation{}, err
	}
	return mutation, nil
}

func decodeOverlayJournalRecordHeader(payload []byte) (OverlayMutation, error) {
	if len(payload) < overlayJournalRecordFloor {
		return OverlayMutation{}, fmt.Errorf("vecindex: overlay journal payload too small")
	}
	return OverlayMutation{
		Kind:              OverlayMutationKind(payload[0]),
		Epoch:             binary.LittleEndian.Uint64(payload[1:9]),
		Sequence:          binary.LittleEndian.Uint64(payload[9:17]),
		ClusterID:         int64(binary.LittleEndian.Uint64(payload[17:25])),
		RowID:             int64(binary.LittleEndian.Uint64(payload[25:33])),
		AppliedAtUnixNano: int64(binary.LittleEndian.Uint64(payload[33:41])),
	}, nil
}

func writeOverlayJournalHeader(file *os.File, epoch, lastSequence uint64) error {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	header := make([]byte, overlayJournalHeaderSize)
	copy(header[:8], overlayJournalMagic)
	binary.LittleEndian.PutUint32(header[8:12], overlayJournalVersion)
	binary.LittleEndian.PutUint64(header[12:20], epoch)
	binary.LittleEndian.PutUint64(header[20:28], lastSequence)
	if _, err := file.Write(header); err != nil {
		return err
	}
	return nil
}
