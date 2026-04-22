package vecindex

import (
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
)

var overlayJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

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
	byCluster    map[int64]map[int64]overlayRow
	rowCluster   map[int64]int64
	tombstones   map[int64]overlayTombstone
}

type overlayRow struct {
	sequence          uint64
	appliedAtUnixNano int64
	vec               []byte
}

type overlayTombstone struct {
	sequence          uint64
	appliedAtUnixNano int64
}

func newOverlaySnapshot(epoch, lastSequence uint64) *OverlaySnapshot {
	return &OverlaySnapshot{
		epoch:        epoch,
		lastSequence: lastSequence,
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
		if !visit(rowID, row.vec) {
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
			if !visit(clusterID, rowID, row.vec) {
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
			bytes += int64(len(row.vec))
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
				Vec:               append([]byte(nil), row.vec...),
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

func (s *OverlaySnapshot) clone() *OverlaySnapshot {
	if s == nil {
		return newOverlaySnapshot(0, 0)
	}
	next := &OverlaySnapshot{
		epoch:        s.epoch,
		lastSequence: s.lastSequence,
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
				vec:               append([]byte(nil), row.vec...),
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

	current := s
	if current == nil {
		current = newOverlaySnapshot(0, 0)
	}
	next := current.clone()
	for i, mutation := range mutations {
		if err := validateOverlayMutation(mutation); err != nil {
			return nil, fmt.Errorf("overlay mutation %d: %w", i, err)
		}
		if mutation.Epoch < next.epoch {
			return nil, fmt.Errorf("overlay mutation %d: epoch %d regresses from %d", i, mutation.Epoch, next.epoch)
		}
		if mutation.Epoch > next.epoch {
			next = newOverlaySnapshot(mutation.Epoch, 0)
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
		next.applyMutation(mutation)
		next.lastSequence = mutation.Sequence
		next.epoch = mutation.Epoch
	}
	return next, nil
}

func (s *OverlaySnapshot) applyMutation(mutation OverlayMutation) {
	switch mutation.Kind {
	case OverlayMutationUpsert:
		s.removeRow(mutation.RowID)
		delete(s.tombstones, mutation.RowID)
		s.upsertRow(mutation.ClusterID, mutation.RowID, mutation.Sequence, mutation.AppliedAtUnixNano, mutation.Vec)
	case OverlayMutationReplace:
		s.removeRow(mutation.RowID)
		s.tombstones[mutation.RowID] = overlayTombstone{
			sequence:          mutation.Sequence,
			appliedAtUnixNano: mutation.AppliedAtUnixNano,
		}
		s.upsertRow(mutation.ClusterID, mutation.RowID, mutation.Sequence, mutation.AppliedAtUnixNano, mutation.Vec)
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

func (s *OverlaySnapshot) upsertRow(clusterID, rowID int64, sequence uint64, appliedAtUnixNano int64, vec []byte) {
	rows := s.byCluster[clusterID]
	if rows == nil {
		rows = make(map[int64]overlayRow)
		s.byCluster[clusterID] = rows
	}
	rows[rowID] = overlayRow{
		sequence:          sequence,
		appliedAtUnixNano: appliedAtUnixNano,
		vec:               append([]byte(nil), vec...),
	}
	s.rowCluster[rowID] = clusterID
}

func validateOverlayMutation(mutation OverlayMutation) error {
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
		if len(mutation.Vec) == 0 {
			return fmt.Errorf("vec must be non-empty for kind %d", mutation.Kind)
		}
	case OverlayMutationDelete:
		if len(mutation.Vec) != 0 {
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
	snapshot, validEnd, err := replayOverlayJournalFile(file)
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
		currentEpoch: snapshot.Epoch(),
		lastSequence: snapshot.LastSequence(),
	}, nil
}

// Replay rebuilds the overlay snapshot from the journal contents.
func (j *OverlayJournal) Replay() (*OverlaySnapshot, error) {
	if j == nil || j.file == nil {
		return newOverlaySnapshot(0, 0), nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	snapshot, validEnd, err := replayOverlayJournalFile(j.file)
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
func (j *OverlayJournal) AppendBatch(mutations []OverlayMutation) error {
	if j == nil {
		return fmt.Errorf("vecindex: overlay journal is nil")
	}
	if len(mutations) == 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	batch, epoch, lastSequence, err := encodeOverlayJournalBatch(j.currentEpoch, j.lastSequence, mutations)
	if err != nil {
		return err
	}
	if _, err := j.file.Write(batch); err != nil {
		return err
	}
	if err := j.file.Sync(); err != nil {
		return err
	}
	j.currentEpoch = epoch
	j.lastSequence = lastSequence
	return nil
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

func (j *OverlayJournal) Rewrite(epoch uint64, lastSequence uint64, mutations []OverlayMutation) error {
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
	if err := writeOverlayJournalHeader(tmp, epoch, lastSequence); err != nil {
		tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	currentEpoch := epoch
	currentSequence := lastSequence
	if len(mutations) > 0 {
		batch, _, lastSequence, err := encodeOverlayJournalBatch(epoch, 0, mutations)
		if err != nil {
			tmp.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		if _, err := tmp.Write(batch); err != nil {
			tmp.Close()
			_ = os.Remove(tmpPath)
			return err
		}
		currentEpoch = mutations[len(mutations)-1].Epoch
		currentSequence = lastSequence
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
	end := int64(overlayJournalHeaderSize)
	if len(mutations) > 0 {
		info, err := file.Stat()
		if err != nil {
			file.Close()
			return err
		}
		end = info.Size()
	}
	if _, err := file.Seek(end, io.SeekStart); err != nil {
		file.Close()
		return err
	}
	j.file = file
	j.currentEpoch = currentEpoch
	j.lastSequence = currentSequence
	return nil
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
	next, err := o.buffer.Snapshot().applyBatch(mutations)
	if err != nil {
		return err
	}
	if err := o.journal.AppendBatch(mutations); err != nil {
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
	o.buffer.Reset(epoch)
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
	mutations := current.MutationsAfter(minSequence)
	next := newOverlaySnapshot(current.Epoch(), current.LastSequence())
	if len(mutations) > 0 {
		var err error
		next, err = newOverlaySnapshot(current.Epoch(), minSequence).applyBatch(mutations)
		if err != nil {
			return err
		}
	}
	if err := o.journal.Rewrite(current.Epoch(), current.LastSequence(), mutations); err != nil {
		return err
	}
	o.buffer.StoreSnapshot(next)
	return nil
}

func (o *JournaledOverlay) Rewrite(epoch uint64, minSequence uint64, mutations []OverlayMutation) error {
	if o == nil || o.journal == nil || o.buffer == nil {
		return fmt.Errorf("vecindex: journaled overlay is not initialized")
	}
	next := newOverlaySnapshot(epoch, minSequence)
	if len(mutations) > 0 {
		var err error
		next, err = next.applyBatch(mutations)
		if err != nil {
			return err
		}
	}
	if err := o.journal.Rewrite(epoch, minSequence, mutations); err != nil {
		return err
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

func encodeOverlayJournalBatch(currentEpoch, currentSequence uint64, mutations []OverlayMutation) ([]byte, uint64, uint64, error) {
	var out []byte
	epoch := currentEpoch
	lastSequence := currentSequence
	for i, mutation := range mutations {
		if err := validateOverlayMutation(mutation); err != nil {
			return nil, 0, 0, fmt.Errorf("overlay mutation %d: %w", i, err)
		}
		if mutation.Epoch < epoch {
			return nil, 0, 0, fmt.Errorf("overlay mutation %d: epoch %d regresses from %d", i, mutation.Epoch, epoch)
		}
		if mutation.Epoch > epoch {
			epoch = mutation.Epoch
			lastSequence = 0
		}
		if mutation.Sequence <= lastSequence {
			return nil, 0, 0, fmt.Errorf("overlay mutation %d: sequence %d must be greater than %d", i, mutation.Sequence, lastSequence)
		}
		payload := encodeOverlayJournalRecord(mutation)
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		out = append(out, lenBuf[:]...)
		out = append(out, payload...)
		var crcBuf [4]byte
		binary.LittleEndian.PutUint32(crcBuf[:], crc32.Checksum(payload, overlayJournalCRCTable))
		out = append(out, crcBuf[:]...)
		epoch = mutation.Epoch
		lastSequence = mutation.Sequence
	}
	return out, epoch, lastSequence, nil
}

func encodeOverlayJournalRecord(mutation OverlayMutation) []byte {
	payload := make([]byte, overlayJournalRecordFloor+len(mutation.Vec))
	payload[0] = byte(mutation.Kind)
	binary.LittleEndian.PutUint64(payload[1:9], mutation.Epoch)
	binary.LittleEndian.PutUint64(payload[9:17], mutation.Sequence)
	binary.LittleEndian.PutUint64(payload[17:25], uint64(mutation.ClusterID))
	binary.LittleEndian.PutUint64(payload[25:33], uint64(mutation.RowID))
	binary.LittleEndian.PutUint64(payload[33:41], uint64(mutation.AppliedAtUnixNano))
	binary.LittleEndian.PutUint32(payload[41:45], uint32(len(mutation.Vec)))
	copy(payload[45:], mutation.Vec)
	return payload
}

func replayOverlayJournalFile(file *os.File) (*OverlaySnapshot, int64, error) {
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
		return newOverlaySnapshot(0, 0), overlayJournalHeaderSize, nil
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

	snapshot := newOverlaySnapshot(0, 0)
	if size == overlayJournalHeaderSize {
		snapshot = newOverlaySnapshot(headerEpoch, headerLastSequence)
	}
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

		payload := make([]byte, recordLen)
		if _, err := io.ReadFull(file, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return snapshot, recordOffset, nil
			}
			return nil, 0, err
		}
		offset += recordLen

		var crcBuf [4]byte
		if _, err := io.ReadFull(file, crcBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return snapshot, recordOffset, nil
			}
			return nil, 0, err
		}
		offset += 4
		if want, got := binary.LittleEndian.Uint32(crcBuf[:]), crc32.Checksum(payload, overlayJournalCRCTable); want != got {
			return nil, 0, fmt.Errorf("vecindex: overlay journal checksum mismatch at offset %d", recordOffset)
		}
		mutation, err := decodeOverlayJournalRecord(payload)
		if err != nil {
			return nil, 0, err
		}
		next, err := snapshot.applyBatch([]OverlayMutation{mutation})
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
