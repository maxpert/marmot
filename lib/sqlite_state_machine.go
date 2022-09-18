package lib

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"github.com/fxamacker/cbor/v2"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
)

type snapshotState = uint8

type appliedIndexInfo struct {
	Index uint64
}

type stateSaveInfo struct {
	appliedIndex appliedIndexInfo
	dbPath       string
}

type SQLiteStateMachine struct {
	NodeID    uint64
	ClusterID uint64
	DB        *db.SqliteStreamDB
	RaftPath  string

	enableSnapshots bool
	snapshotLock    *sync.Mutex
	snapshotState   snapshotState
	applied         *appliedIndexInfo
}

type ReplicationEvent[T any] struct {
	FromNodeId uint64
	Payload    *T
}

const (
	snapshotNotInitialized snapshotState = 0
	snapshotSaved          snapshotState = 1
	snapshotRestored       snapshotState = 2
)

func (e *ReplicationEvent[T]) Marshal() ([]byte, error) {
	return cbor.Marshal(e)
}

func (e *ReplicationEvent[T]) Unmarshal(data []byte) error {
	return cbor.Unmarshal(data, e)
}

func NewDBStateMachine(
	clusterID, nodeID uint64,
	db *db.SqliteStreamDB,
	path string,
	enableSnapshots bool,
) *SQLiteStateMachine {
	return &SQLiteStateMachine{
		DB:        db,
		NodeID:    nodeID,
		ClusterID: clusterID,
		RaftPath:  path,

		enableSnapshots: enableSnapshots,
		snapshotLock:    &sync.Mutex{},
		snapshotState:   snapshotNotInitialized,
		applied:         &appliedIndexInfo{Index: 0},
	}
}

func (ssm *SQLiteStateMachine) Open(_ <-chan struct{}) (uint64, error) {
	err := ssm.readIndex()
	if err != nil {
		return 0, err
	}

	return ssm.applied.Index, nil
}

func (ssm *SQLiteStateMachine) Update(entries []sm.Entry) ([]sm.Entry, error) {
	for _, entry := range entries {
		event := &ReplicationEvent[db.ChangeLogEvent]{}
		if err := event.Unmarshal(entry.Cmd); err != nil {
			log.Error().Err(err).Msg("Unable to unmarshal command")
			return nil, err
		}

		logger := log.With().
			Int64("table_id", event.Payload.Id).
			Str("table_name", event.Payload.TableName).
			Str("type", event.Payload.Type).
			Logger()

		err := ssm.DB.Replicate(event.Payload)
		if err != nil {
			logger.Error().Err(err).Msg("Row not replicated...")
			return nil, err
		}

		ssm.applied.Index = entry.Index
		if err := ssm.saveIndex(); err != nil {
			return nil, err
		}

		entry.Result = sm.Result{Value: entry.Index}
	}

	return entries, nil
}

func (ssm *SQLiteStateMachine) Sync() error {
	return nil
}

func (ssm *SQLiteStateMachine) PrepareSnapshot() (interface{}, error) {
	log.Debug().
		Uint64("cluster", ssm.ClusterID).
		Uint64("node", ssm.NodeID).
		Bool("enabled", ssm.enableSnapshots).
		Msg("Preparing snapshot...")

	if !ssm.enableSnapshots {
		return stateSaveInfo{dbPath: "", appliedIndex: *ssm.applied}, nil
	}

	bkFileDir, err := ssm.getSnapshotDir()
	if err != nil {
		return nil, err
	}

	bkFilePath := path.Join(bkFileDir, "backup.sqlite")
	err = ssm.DB.BackupTo(bkFilePath)
	if err != nil {
		return nil, err
	}

	return stateSaveInfo{dbPath: bkFilePath, appliedIndex: *ssm.applied}, nil
}

func (ssm *SQLiteStateMachine) SaveSnapshot(st interface{}, writer io.Writer, _ <-chan struct{}) error {
	log.Debug().
		Uint64("cluster", ssm.ClusterID).
		Uint64("node", ssm.NodeID).
		Bool("enabled", ssm.enableSnapshots).
		Msg("Saving snapshot...")

	stInfo, ok := st.(stateSaveInfo)
	if !ok {
		return fmt.Errorf(fmt.Sprintf("invalid save state info %v", st))
	}

	mBytes, err := cbor.Marshal(stInfo.appliedIndex)
	err = writeUint32(writer, uint32(len(mBytes)))
	if err != nil {
		return err
	}

	_, err = writer.Write(mBytes)
	if err != nil {
		return err
	}

	// Write length of filepath as indicator for following up stream
	err = writeUint32(writer, uint32(len(stInfo.dbPath)))
	if err != nil {
		return err
	}

	if stInfo.dbPath != "" {
		filepath := stInfo.dbPath
		fi, err := os.Open(filepath)
		if err != nil {
			return err
		}
		defer ssm.cleanup(fi, filepath)

		_, err = io.Copy(writer, fi)
		if err != nil {
			return err
		}
	}

	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()
	ssm.snapshotState = snapshotSaved
	return nil
}

func (ssm *SQLiteStateMachine) RecoverFromSnapshot(reader io.Reader, _ <-chan struct{}) error {
	log.Debug().
		Uint64("cluster", ssm.ClusterID).
		Uint64("node", ssm.NodeID).
		Bool("enabled", ssm.enableSnapshots).
		Msg("Recovering from snapshot...")

	appIndex := appliedIndexInfo{}
	buffLen, err := readUint32(reader)
	if err != nil {
		return err
	}

	dec := cbor.NewDecoder(io.LimitReader(reader, int64(buffLen)))
	err = dec.Decode(&appIndex)
	if err != nil {
		return err
	}

	hasData, err := readUint32(reader)
	if err != nil {
		return err
	}

	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()
	if hasData != 0 {
		basePath, err := ssm.getSnapshotDir()
		if err != nil {
			return err
		}

		filepath := path.Join(basePath, "restore.sqlite")
		fo, err := os.OpenFile(filepath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer ssm.cleanup(fo, filepath)

		_, err = io.Copy(fo, reader)
		if err != nil {
			return err
		}

		// Flush file contents before handing off
		err = fo.Sync()
		if err != nil {
			return err
		}

		err = ssm.importSnapshot(filepath)
		if err != nil {
			return err
		}
	}

	ssm.applied = &appIndex
	return ssm.saveIndex()
}

func (ssm *SQLiteStateMachine) Lookup(_ interface{}) (interface{}, error) {
	return 0, nil
}

func (ssm *SQLiteStateMachine) IsSnapshotEnabled() bool {
	return ssm.enableSnapshots
}

func (ssm *SQLiteStateMachine) HasRestoredSnapshot() bool {
	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()

	return ssm.snapshotState == snapshotRestored
}

func (ssm *SQLiteStateMachine) HasSavedSnapshot() bool {
	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()

	return ssm.snapshotState == snapshotSaved
}

func (ssm *SQLiteStateMachine) Close() error {
	return nil
}

func (ssm *SQLiteStateMachine) importSnapshot(filepath string) error {
	log.Info().Str("path", filepath).Msg("Importing...")
	err := ssm.DB.RestoreFrom(filepath)
	if err != nil {
		return err
	}

	log.Info().Str("path", filepath).Msg("Snapshot imported")
	ssm.snapshotState = snapshotRestored
	return nil
}

func (ssm *SQLiteStateMachine) getSnapshotDir() (string, error) {
	tmpPath := path.Join(ssm.RaftPath, fmt.Sprintf("marmot-%d-%d", ssm.ClusterID, ssm.NodeID))
	err := os.MkdirAll(tmpPath, 0744)
	if err != nil {
		log.Error().Err(err).Msg("Unable to create directory for snapshot")
		return "", err
	}

	return tmpPath, nil
}

func (ssm *SQLiteStateMachine) cleanup(f *os.File, filepath string) {
	if err := f.Close(); err != nil {
		log.Warn().Err(err).Str("path", filepath).Msg("Unable to close snapshot file")
	}

	err := os.Remove(filepath)
	if err != nil {
		log.Error().Err(err).Str("path", filepath).Msg("Unable to cleanup snapshot file")
	}
}

func (ssm *SQLiteStateMachine) saveIndex() error {
	filepath, err := ssm.getIndexFilePath()
	if err != nil {
		return err
	}

	fo, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0644)
	if err != nil {
		return err
	}
	defer func() { _ = fo.Close() }()

	_, err = fo.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	b, err := cbor.Marshal(ssm.applied)
	if err != nil {
		return err
	}

	_, err = fo.Write(b)
	if err != nil {
		return err
	}

	err = fo.Sync()
	if err != nil {
		return err
	}

	log.Debug().
		Uint64("node_id", ssm.NodeID).
		Uint64("cluster_id", ssm.ClusterID).
		Uint64("index", ssm.applied.Index).
		Msg("Saved index")
	return nil
}

func (ssm *SQLiteStateMachine) readIndex() error {
	filepath, err := ssm.getIndexFilePath()
	if err != nil {
		return err
	}

	fi, err := os.OpenFile(filepath, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}
	defer func() { _ = fi.Close() }()

	b, err := io.ReadAll(fi)
	if err != nil {
		return err
	}

	err = cbor.Unmarshal(b, ssm.applied)
	if err != nil {
		return err
	}

	return nil
}

func (ssm *SQLiteStateMachine) getIndexFilePath() (string, error) {
	basePath, err := ssm.getSnapshotDir()
	if err != nil {
		return "", err
	}

	filepath := path.Join(basePath, "current-index")
	return filepath, nil
}
