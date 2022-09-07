package lib

import (
	"database/sql"
	"fmt"

	_ "embed"

	"github.com/doug-martin/goqu/v9"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/raftio"
	"github.com/lni/dragonboat/v3/raftpb"
	_ "github.com/mattn/go-sqlite3"
	"github.com/samber/lo"
	"marmot/db"
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

func (s *SQLiteLogDB) SaveRaftState(updates []raftpb.Update, shardID uint64) error {
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

			if len(upd.EntriesToSave) > 0 {
				for _, entry := range upd.EntriesToSave {
					// nodeID, clusterID, entry.Index
					err := saveInfoTuple(tx, &entry.Index, upd.NodeID, upd.ClusterID, Entry, entry.Marshal)
					if err != nil {
						return err
					}
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

	currentSize := uint64(0)
	ret := make([]raftpb.Entry, 0)
	for eRow.Next() {
		bts := make([]byte, 0)
		err = eRow.Scan(&bts)
		if err != nil {
			return entries, size, err
		}

		e := raftpb.Entry{}
		err = e.Unmarshal(bts)
		if err != nil {
			return entries, size, err
		}

		ret = append(ret, e)
		currentSize += uint64(e.SizeUpperLimit())
		if currentSize > maxSize {
			break
		}
	}

	if len(ret) == 0 {
		return entries, size, nil
	}

	return ret, currentSize, nil
}

func (s *SQLiteLogDB) ReadRaftState(clusterID uint64, nodeID uint64, snapshotIndex uint64) (raftio.RaftState, error) {
	entry := raftInfoEntry{}
	ret := raftio.RaftState{}

	ok, err := s.db.From(raftInfoTable).
		Where(goqu.Ex{
			"entry_index": snapshotIndex,
			"node_id":     nodeID,
			"cluster_id":  clusterID,
			"entry_type":  State,
		}).ScanStruct(&entry)

	if err != nil {
		return ret, err
	}

	if !ok {
		return ret, raftio.ErrNoSavedLog
	}

	firstIndex, entriesCount, err := s.getRange(nodeID, clusterID)
	if err != nil {
		return ret, err
	}

	err = ret.State.Unmarshal(entry.Payload)
	if err != nil {
		return ret, err
	}

	ret.FirstIndex = firstIndex
	ret.EntryCount = entriesCount

	return ret, nil
}

// RemoveEntriesTo removes entries associated with the specified Raft node up
// to the specified index.
func (s *SQLiteLogDB) RemoveEntriesTo(clusterID uint64, nodeID uint64, index uint64) error {
	return s.db.WithTx(func(tx *goqu.TxDatabase) error {
		return deleteInfoTuple(tx, nodeID, clusterID, Entry, []goqu.Expression{
			goqu.C("entry_index").Lte(index),
		})
	})
}

func (s *SQLiteLogDB) CompactEntriesTo(clusterID uint64, nodeID uint64, index uint64) (<-chan struct{}, error) {
	ch := make(chan struct{})
	err := s.DeleteSnapshot(clusterID, nodeID, index)
	if err != nil {
		return nil, err
	}

	defer func() {
		close(ch)
	}()

	return nil, err
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
	entries := make([]raftInfoEntry, 0)
	err := s.db.
		From(raftInfoTable).
		Where(
			goqu.C("node_id").Eq(nodeID),
			goqu.C("cluster_id").Eq(clusterID),
			goqu.C("entry_type").Eq(Snapshot),
			goqu.C("entry_index").Lte(index)).
		Order(goqu.C("entry_index").Asc()).
		Prepared(true).
		ScanStructs(&entries)

	if err != nil {
		return nil, err
	}

	ret := make([]raftpb.Snapshot, 0)
	for _, entry := range entries {
		snp := raftpb.Snapshot{}
		err = snp.Unmarshal(entry.Payload)
		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (s *SQLiteLogDB) ImportSnapshot(_ raftpb.Snapshot, _ uint64) error {
	return nil
}

func (s *SQLiteLogDB) getRange(nodeID, clusterID uint64) (uint64, uint64, error) {
	count := uint64(0)
	ok, err := s.db.From(raftInfoTable).Select(
		goqu.COUNT(goqu.C("entry_index")),
	).
		Where(goqu.Ex{"node_id": nodeID, "cluster_id": clusterID}).
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
		goqu.MIN(goqu.C("entry_index")),
	).
		Where(goqu.Ex{"node_id": nodeID, "cluster_id": clusterID}).
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
	exps := []goqu.Expression{
		goqu.C("node_id").Eq(nodeID),
		goqu.C("cluster_id").Eq(clusterID),
		goqu.C("entry_type").Eq(entryType),
	}

	exps = append(exps, additionalExpressions...)

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
	db *goqu.TxDatabase,
	index *uint64,
	nodeID uint64,
	clusterID uint64,
	entryType raftInfoEntryType,
	f func() ([]byte, error),
) error {
	data, err := f()
	if err != nil {
		return err
	}

	query := fmt.Sprintf(`
			INSERT OR REPLACE INTO %s(entry_index, node_id, cluster_id, entry_type, payload)
			VALUES(?, ?, ?, ?, ?);`, raftInfoTable)
	_, err = db.Exec(query, index, nodeID, clusterID, entryType, data)
	return err
}
