package lib

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	_ "embed"

	"github.com/doug-martin/goqu/v9"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type raftInfoEntryType = int16

type SQLiteLogDB struct {
	name string
	db   *goqu.Database
}

type raftInfoEntry struct {
	NodeId    uint64            `db:"node_id"`
	ClusterId uint64            `db:"cluster_id"`
	Index     *uint64           `db:"entry_index"`
	Type      raftInfoEntryType `db:"entry_type"`
	Payload   []byte            `db:"payload"`
}

type SQLiteLogDBFactory struct {
	metaPath string
	nodeID   uint64
	path     string
}

const raftInfoTable = "raft_info"

const (
	State     raftInfoEntryType = 1
	Entry     raftInfoEntryType = 2
	Bootstrap raftInfoEntryType = 3
	Snapshot  raftInfoEntryType = 4
)

//go:embed log_db_script.sql
var logDBScript string
var errIndexNotApplicable = errors.New("index can not be applied for this save type")

func NewSQLiteLogDBFactory(metaPath string, nodeID uint64) *SQLiteLogDBFactory {
	path := fmt.Sprintf("%s/logdb-%d.sqlite?_journal=wal", metaPath, nodeID)
	return &SQLiteLogDBFactory{
		metaPath: metaPath,
		nodeID:   nodeID,
		path:     path,
	}
}

func (f *SQLiteLogDBFactory) Name() string {
	return f.metaPath
}

func (f *SQLiteLogDBFactory) Create(
	_ config.NodeHostConfig,
	_ config.LogDBCallback,
	_ []string,
	_ []string,
) (raftio.ILogDB, error) {
	logDB, err := newSQLiteLogDB(f.path)
	if err != nil {
		return nil, err
	}

	return logDB, err
}

func newSQLiteLogDB(path string) (*SQLiteLogDB, error) {
	conn, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	_, err = conn.Exec(logDBScript)
	if err != nil {
		return nil, err
	}

	return &SQLiteLogDB{
		name: path,
		db:   goqu.New("sqlite", conn),
	}, nil
}

func (s *SQLiteLogDB) Name() string {
	return s.name
}

func (s *SQLiteLogDB) Close() {
}

func (s *SQLiteLogDB) BinaryFormat() uint32 {
	return 1
}

// ListNodeInfo lists all available NodeInfo found in the log DB.
func (s *SQLiteLogDB) ListNodeInfo() ([]raftio.NodeInfo, error) {
	arr := make([]raftInfoEntry, 0)
	err := s.db.Select("node_id", "column_id").
		Distinct().
		From(raftInfoTable).
		Prepared(true).
		ScanStructs(&arr)
	if err != nil {
		return nil, err
	}

	nodes := lo.Map(arr, func(e raftInfoEntry, i int) raftio.NodeInfo {
		return raftio.NodeInfo{NodeID: e.NodeId, ClusterID: e.ClusterId}
	})

	return nodes, nil
}

func (s *SQLiteLogDB) SaveBootstrapInfo(clusterID uint64, nodeID uint64, bootstrap raftpb.Bootstrap) error {
	data, err := bootstrap.Marshal()
	if err != nil {
		return err
	}

	_, err = s.db.Insert(raftInfoTable).
		Rows(&raftInfoEntry{
			NodeId:    nodeID,
			ClusterId: clusterID,
			Type:      Bootstrap,
			Payload:   data,
		}).
		Prepared(true).
		Executor().
		Exec()

	return err
}

func (s *SQLiteLogDB) GetBootstrapInfo(clusterID uint64, nodeID uint64) (raftpb.Bootstrap, error) {
	data := raftInfoEntry{}
	bs := raftpb.Bootstrap{}
	hasRow, err := s.db.
		From(raftInfoTable).
		Where(goqu.Ex{"cluster_id": clusterID, "node_id": nodeID, "entry_type": Bootstrap}).
		Prepared(true).
		ScanStruct(&data)

	if err != nil {
		return bs, err
	}

	if !hasRow {
		return bs, raftio.ErrNoBootstrapInfo
	}

	err = bs.Unmarshal(data.Payload)
	if err != nil {
		return bs, err
	}

	return bs, nil
}

func (s *SQLiteLogDB) SaveRaftState(updates []raftpb.Update, _ uint64) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		for _, upd := range updates {
			if !raftpb.IsEmptyState(upd.State) {
				err := saveInfoTuple(tx, nil, upd.NodeID, upd.ClusterID, State, upd.State.Marshal)
				if err != nil {
					return err
				}
			}

			if !raftpb.IsEmptySnapshot(upd.Snapshot) {
				if len(upd.EntriesToSave) > 0 {
					lastIndex := upd.EntriesToSave[len(upd.EntriesToSave)-1].Index
					if upd.Snapshot.Index > lastIndex {
						return fmt.Errorf("max index not handled, %d, %d", upd.Snapshot.Index, lastIndex)
					}
				}

				err := saveInfoTuple(tx, &upd.Snapshot.Index, upd.NodeID, upd.ClusterID, Snapshot, upd.Snapshot.Marshal)
				if err != nil {
					return err
				}
			}

			for _, entry := range upd.EntriesToSave {
				err := saveInfoTuple(tx, &entry.Index, upd.NodeID, upd.ClusterID, Entry, entry.Marshal)
				if err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *SQLiteLogDB) RemoveNodeData(clusterID uint64, nodeID uint64) error {
	_, err := s.db.Delete(raftInfoTable).Where(goqu.Ex{
		"cluster_id": clusterID,
		"node_id":    nodeID,
	}).Prepared(true).Executor().Exec()

	if err != nil {
		return err
	}

	return err
}

// IterateEntries returns the continuous Raft log entries of the specified
// Raft node between the index value range of [low, high) up to a max size
// limit of maxSize bytes. It returns the located log entries, their total
// size in bytes and the occurred error.
func (s *SQLiteLogDB) IterateEntries(
	entries []raftpb.Entry,
	size uint64,
	clusterID uint64,
	nodeID uint64,
	low uint64,
	high uint64,
	maxSize uint64,
) ([]raftpb.Entry, uint64, error) {
	logger := log.With().
		Uint64("low", low).
		Uint64("size", size).
		Uint64("high", high).
		Uint64("node_id", nodeID).
		Uint64("max_size", maxSize).
		Uint64("cluster_id", clusterID).
		Uint64("entries", uint64(len(entries))).
		Logger()

	min, count, err := s.getEntryRange(nodeID, clusterID)
	if err == raftio.ErrNoSavedLog {
		logger.Warn().Msg("No entries...")
		return entries, size, nil
	}

	if err != nil {
		logger.Warn().Err(err).Msg("Range error")
		return entries, size, err
	}

	logger = logger.With().Uint64("min", min).Uint64("max", min+count-1).Logger()
	rows, err := s.db.
		Select("payload").
		From(raftInfoTable).
		Where(
			goqu.C("node_id").Eq(nodeID),
			goqu.C("cluster_id").Eq(clusterID),
			goqu.C("entry_type").Eq(Entry),
			goqu.C("entry_index").Gte(low),
			goqu.C("entry_index").Lt(high)).
		Order(goqu.C("entry_index").Asc()).
		Prepared(true).
		Executor().Query()

	if err != nil {
		return entries, size, err
	}
	eRow := &db.EnhancedRows{Rows: rows}
	defer eRow.Finalize()

	expectedIndex := low
	buff := make([]byte, 0)
	for eRow.Next() {
		err = eRow.Scan(&buff)
		if err != nil {
			return entries, size, err
		}

		e := raftpb.Entry{}
		err = e.Unmarshal(buff)
		if err != nil {
			return entries, size, err
		}
		if e.Index != expectedIndex {
			logger.Warn().Msg(fmt.Sprintf("Index mismatch %d != %d", e.Index, expectedIndex))
			break
		}

		size += uint64(e.SizeUpperLimit())
		entries = append(entries, e)
		expectedIndex++

		if size >= maxSize {
			logger.Trace().Msg(fmt.Sprintf("Size mismatch %d != %d", size, maxSize))
			break
		}
	}

	logger.Trace().Msg("Scan complete")
	return entries, size, nil
}

func (s *SQLiteLogDB) ReadRaftState(clusterID uint64, nodeID uint64, snapshotIndex uint64) (raftio.RaftState, error) {
	entry := raftInfoEntry{}
	ret := raftio.RaftState{}
	log.Trace().Msg(fmt.Sprintf("ReadRaftState %d %d %d", clusterID, nodeID, snapshotIndex))

	firstIndex, entriesCount, err := s.getEntryRange(nodeID, clusterID)
	if err != nil {
		return ret, err
	}

	ok, err := s.db.From(raftInfoTable).
		Where(goqu.Ex{
			"node_id":    nodeID,
			"cluster_id": clusterID,
			"entry_type": State,
		}).ScanStruct(&entry)

	if err != nil {
		return ret, err
	}

	if !ok {
		return ret, raftio.ErrNoSavedLog
	}

	err = ret.State.Unmarshal(entry.Payload)
	if err != nil {
		return ret, err
	}

	ret.FirstIndex = firstIndex
	ret.EntryCount = entriesCount

	// Have to investigate but existing code in dragonboat suggests with 1 entry it returns 0
	if snapshotIndex == (firstIndex + entriesCount - 1) {
		ret.EntryCount = 0
	}

	return ret, nil
}

// RemoveEntriesTo removes entries associated with the specified Raft node up
// to the specified index.
func (s *SQLiteLogDB) RemoveEntriesTo(clusterID uint64, nodeID uint64, index uint64) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		log.Trace().Msg(fmt.Sprintf("RemoveEntriesTo c: %d n: %d i: %d", clusterID, nodeID, index))
		return deleteInfoTuple(tx, nodeID, clusterID, Entry, []goqu.Expression{
			goqu.C("entry_index").Lt(index),
		})
	})
}

func (s *SQLiteLogDB) CompactEntriesTo(clusterID uint64, nodeID uint64, index uint64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	log.Trace().Msg(fmt.Sprintf("CompactEntriesTo c: %d n: %d i: %d", clusterID, nodeID, index))

	defer func() {
		var rows *sql.Rows
		var err error

		for {
			if rows != nil {
				_ = rows.Close()
			}

			rows, err = s.db.Query("PRAGMA wal_checkpoint(TRUNCATE)")
			if err != nil {
				log.Error().Err(err).Msg("Unable to compact checkpoint")
				break
			}

			rows.Next()

			var busy, logi, checkpointed int64
			err = rows.Scan(&busy, &logi, &checkpointed)
			if err != nil {
				log.Error().Err(err).Msg("Unable to read checkpoint data")
				break
			}

			if busy == 0 {
				log.Info().
					Int64("log_index", logi).
					Int64("checkpointed", checkpointed).
					Msg("Checkpoint complete")
				break
			}

			time.Sleep(200 * time.Millisecond)
		}

		if rows != nil {
			_ = rows.Close()
		}
		close(ch)
	}()

	return ch, nil
}

func (s *SQLiteLogDB) SaveSnapshots(updates []raftpb.Update) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		for _, upd := range updates {
			err := saveInfoTuple(tx, &upd.Snapshot.Index, upd.NodeID, upd.ClusterID, Snapshot, upd.Snapshot.Marshal)
			if err != nil {
				return err
			}
		}

		return nil
	})
}

func (s *SQLiteLogDB) DeleteSnapshot(clusterID uint64, nodeID uint64, index uint64) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		return deleteInfoTuple(tx, nodeID, clusterID, Snapshot, []goqu.Expression{
			goqu.C("entry_index").Eq(index),
		})
	})
}

func (s *SQLiteLogDB) ListSnapshots(clusterID uint64, nodeID uint64, index uint64) ([]raftpb.Snapshot, error) {
	exps := []goqu.Expression{
		goqu.C("node_id").Eq(nodeID),
		goqu.C("cluster_id").Eq(clusterID),
		goqu.C("entry_type").Eq(Snapshot),
	}

	if index != math.MaxUint64 {
		exps = append(exps, goqu.C("entry_index").Lte(index))
	}

	rows, err := s.db.
		Select("payload").
		From(raftInfoTable).
		Where(exps...).
		Order(goqu.C("entry_index").Asc()).
		Prepared(true).
		Executor().
		Query()

	if err != nil {
		return nil, err
	}

	eRows := db.EnhancedRows{Rows: rows}
	defer eRows.Finalize()

	ret := make([]raftpb.Snapshot, 0)
	buf := make([]byte, 0)
	for eRows.Next() {
		err = eRows.Scan(&buf)
		if err != nil {
			return nil, err
		}

		snp := raftpb.Snapshot{}
		err = snp.Unmarshal(buf)
		if err != nil {
			return nil, err
		}

		ret = append(ret, snp)
	}

	return ret, nil
}

func (s *SQLiteLogDB) ImportSnapshot(snp raftpb.Snapshot, nodeID uint64) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		if raftpb.IsEmptySnapshot(snp) {
			return nil
		}

		// Replace Bootstrap
		err := deleteInfoTuple(tx, nodeID, snp.ClusterId, Bootstrap, []goqu.Expression{})
		if err != nil {
			return err
		}

		bootstrap := raftpb.Bootstrap{
			Join: true,
			Type: snp.Type,
		}
		err = saveInfoTuple(tx, nil, nodeID, snp.ClusterId, Bootstrap, bootstrap.Marshal)
		if err != nil {
			return err
		}

		// Replace state
		err = deleteInfoTuple(tx, nodeID, snp.ClusterId, State, []goqu.Expression{})
		if err != nil {
			return err
		}

		state := raftpb.State{
			Term:   snp.Term,
			Commit: snp.Index,
		}
		err = saveInfoTuple(tx, nil, nodeID, snp.ClusterId, State, state.Marshal)
		if err != nil {
			return err
		}

		// Delete snapshot log entries ahead of index
		err = deleteInfoTuple(tx, nodeID, snp.ClusterId, Snapshot, []goqu.Expression{
			goqu.C("entry_index").Gte(snp.Index),
		})
		if err != nil {
			return err
		}

		err = saveInfoTuple(tx, &snp.Index, nodeID, snp.ClusterId, Snapshot, snp.Marshal)
		if err != nil {
			return err
		}

		return nil
	})
}

func (s *SQLiteLogDB) getEntryRange(nodeID, clusterID uint64) (uint64, uint64, error) {
	count := uint64(0)
	ok, err := s.db.From(raftInfoTable).Select(
		goqu.COUNT("entry_index"),
	).
		Where(goqu.Ex{"node_id": nodeID, "cluster_id": clusterID, "entry_type": Entry}).
		Prepared(true).
		Executor().
		ScanVal(&count)

	if err != nil {
		return 0, 0, err
	}

	if !ok {
		return 0, 0, raftio.ErrNoSavedLog
	}

	if count <= 0 {
		return 0, 0, nil
	}

	min := uint64(0)
	ok, err = s.db.From(raftInfoTable).Select(
		goqu.MIN("entry_index"),
	).
		Where(goqu.Ex{"node_id": nodeID, "cluster_id": clusterID, "entry_type": Entry}).
		Prepared(true).
		Executor().
		ScanVal(&min)

	if err != nil {
		return 0, 0, err
	}

	if !ok {
		return 0, 0, raftio.ErrNoSavedLog
	}

	return min, count, nil
}

func deleteInfoTuple(
	db *goqu.TxDatabase,
	nodeID uint64,
	clusterID uint64,
	entryType raftInfoEntryType,
	additionalExpressions []goqu.Expression,
) error {

	logger := log.With().
		Uint64("node_id", nodeID).
		Uint64("cluster_id", clusterID).
		Uint64("entry_type", uint64(entryType)).
		Logger()

	for i, x := range additionalExpressions {
		logger = logger.With().Str(fmt.Sprintf("exp[%d]", i), fmt.Sprintf("%v", x)).Logger()
	}

	exps := []goqu.Expression{
		goqu.C("node_id").Eq(nodeID),
		goqu.C("cluster_id").Eq(clusterID),
		goqu.C("entry_type").Eq(entryType),
	}

	exps = append(exps, additionalExpressions...)

	logger.Trace().Msg("Deleted rows")
	_, err := db.Delete(raftInfoTable).
		Where(exps...).
		Prepared(true).
		Executor().
		Exec()

	if err != nil {
		return err
	}
	return nil
}

func saveInfoTuple(
	tx *goqu.TxDatabase,
	index *uint64,
	nodeID uint64,
	clusterID uint64,
	entryType raftInfoEntryType,
	f func() ([]byte, error),
) error {
	// Assert that BootStrap and State has no index
	if entryType == Bootstrap || entryType == State {
		if index != nil {
			return errIndexNotApplicable
		}
	}

	data, err := f()
	if err != nil {
		return err
	}

	exps := []goqu.Expression{
		goqu.C("node_id").Eq(nodeID),
		goqu.C("cluster_id").Eq(clusterID),
		goqu.C("entry_type").Eq(entryType),
	}

	logger := log.With().
		Uint64("node_id", nodeID).
		Uint64("cluster_id", clusterID).
		Uint64("entry_type", uint64(entryType)).
		Logger()

	if index != nil {
		exps = append(exps, goqu.C("entry_index").Eq(*index))
		logger = logger.With().Uint64("index", *index).Logger()
	} else {
		exps = append(exps, goqu.C("entry_index").IsNull())
		logger = logger.With().Int("index", -1).Logger()
	}

	_, err = tx.Delete(raftInfoTable).Where(exps...).Prepared(true).Executor().Exec()
	if err != nil {
		return err
	}

	_, err = tx.Insert(raftInfoTable).Rows(goqu.Record{
		"node_id":     nodeID,
		"entry_index": index,
		"cluster_id":  clusterID,
		"entry_type":  entryType,
		"payload":     data,
	}).Prepared(true).Executor().Exec()

	logger.Trace().Err(err).Msg("Saved")
	return err
}
