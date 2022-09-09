package lib

import (
	"fmt"
	"io"
	"os"
	"path"
	"sync"

	"marmot/db"

	"github.com/fxamacker/cbor/v2"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/rs/zerolog/log"
)

type snapshotState = uint8

type SQLiteStateMachine struct {
	NodeID        uint64
	DB            *db.SqliteStreamDB
	SnapshotsPath string
	snapshotLock  *sync.Mutex
	snapshotState snapshotState
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

func (ssm *SQLiteStateMachine) Update(bytes []byte) (sm.Result, error) {
	event := &ReplicationEvent[db.ChangeLogEvent]{}
	if err := event.Unmarshal(bytes); err != nil {
		return sm.Result{}, err
	}

	logger := log.With().
		Int64("table_id", event.Payload.Id).
		Str("table_name", event.Payload.TableName).
		Str("type", event.Payload.Type).
		Logger()

	if event.FromNodeId == ssm.NodeID {
		return sm.Result{Value: 0}, nil
	}

	err := ssm.DB.Replicate(event.Payload)
	if err != nil {
		logger.Error().Err(err).Msg("Row not replicated...")
		return sm.Result{Value: 0}, nil
	}

	logger.Debug().Msg("Replicated row")
	return sm.Result{Value: 1}, nil
}

func (ssm *SQLiteStateMachine) Lookup(_ interface{}) (interface{}, error) {
	return 0, nil
}

func (ssm *SQLiteStateMachine) SaveSnapshot(_ io.Writer, files sm.ISnapshotFileCollection, _ <-chan struct{}) error {
	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()

	bkFileDir, err := ssm.GetSnapshotDir()
	if err != nil {
		return err
	}

	bkFilePath := path.Join(bkFileDir, "backup.sqlite")
	err = ssm.DB.BackupTo(bkFilePath)
	if err != nil {
		return err
	}

	files.AddFile(0, bkFilePath, nil)
	ssm.snapshotState = snapshotSaved
	log.Info().Str("path", bkFilePath).Msg("Snapshot saved")
	return nil
}

func (ssm *SQLiteStateMachine) RecoverFromSnapshot(_ io.Reader, sps []sm.SnapshotFile, _ <-chan struct{}) error {
	if len(sps) < 1 {
		return fmt.Errorf("not file snapshots to restore")
	}

	err := ssm.ImportSnapshot(sps[0].Filepath)
	if err != nil {
		return err
	}

	return nil
}

func (ssm *SQLiteStateMachine) ImportSnapshot(filepath string) error {
	ssm.snapshotLock.Lock()
	defer ssm.snapshotLock.Unlock()

	err := ssm.DB.RestoreFrom(filepath)
	if err != nil {
		return err
	}

	log.Info().Str("path", filepath).Msg("Snapshot imported")
	ssm.snapshotState = snapshotRestored
	return nil
}

func (ssm *SQLiteStateMachine) GetSnapshotDir() (string, error) {
	tmpPath := path.Join(ssm.SnapshotsPath, "marmot", "snapshot")
	err := os.MkdirAll(tmpPath, 0744)
	if err != nil {
		log.Error().Err(err).Msg("Unable to create directory for snapshot")
		return "", err
	}

	return tmpPath, nil
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

func NewDBStateMachine(nodeID uint64, db *db.SqliteStreamDB, path string) *SQLiteStateMachine {
	return &SQLiteStateMachine{
		DB:            db,
		NodeID:        nodeID,
		SnapshotsPath: path,
		snapshotLock:  &sync.Mutex{},
	}
}
