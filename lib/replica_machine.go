package lib

import (
    "io"

    "github.com/fxamacker/cbor/v2"
    sm "github.com/lni/dragonboat/v3/statemachine"
    "github.com/rs/zerolog/log"
    "marmot/db"
)

type SQLiteStateMachine struct {
    NodeID    uint64
    DB        *db.SqliteStreamDB
    lastIndex uint64
}

type ReplicationEvent[T any] struct {
    FromNodeId uint64
    Payload    *T
}

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

func (ssm *SQLiteStateMachine) SaveSnapshot(_ io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
    err := ssm.DB.CleanupChangeLogs()
    if err != nil {
        return err
    }

    return nil
}

func (ssm *SQLiteStateMachine) RecoverFromSnapshot(_ io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
    return nil
}

func (ssm *SQLiteStateMachine) Close() error {
    return nil
}

func NewDBStateMachine(shardID uint64, db *db.SqliteStreamDB) sm.IStateMachine {
    return &SQLiteStateMachine{
        DB:        db,
        NodeID:    shardID,
        lastIndex: 0,
    }
}
