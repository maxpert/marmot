package db

import (
    "io"

    "github.com/fxamacker/cbor/v2"
    sm "github.com/lni/dragonboat/v3/statemachine"
    "github.com/rs/zerolog/log"
)

type SQLiteStateMachine struct {
    NodeID    uint64
    DB        *SqliteStreamDB
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
    event := &ReplicationEvent[ChangeLogEvent]{}
    if err := event.Unmarshal(bytes); err != nil {
        return sm.Result{}, err
    }

    logger := log.With().
        Int64("change_row_id", event.Payload.ChangeRowId).
        Int64("row_id", event.Payload.Id).
        Str("table_name", event.Payload.TableName).
        Logger()

    logger.Debug().Msg("Propagating...")
    if event.FromNodeId != ssm.NodeID {
        err := ssm.DB.Replicate(event.Payload)
        if err != nil {
            logger.Error().Err(err).Msg("Change not applied...")
        }
    }

    return sm.Result{Value: 1}, nil
}

func (ssm *SQLiteStateMachine) Lookup(_ interface{}) (interface{}, error) {
    return 0, nil
}

func (ssm *SQLiteStateMachine) SaveSnapshot(_ io.Writer, _ sm.ISnapshotFileCollection, _ <-chan struct{}) error {
    return nil
}

func (ssm *SQLiteStateMachine) RecoverFromSnapshot(_ io.Reader, _ []sm.SnapshotFile, _ <-chan struct{}) error {
    return nil
}

func (ssm *SQLiteStateMachine) Close() error {
    return nil
}

func NewDBStateMachine(shardID uint64, db *SqliteStreamDB) sm.IStateMachine {
    return &SQLiteStateMachine{
        DB:        db,
        NodeID:    shardID,
        lastIndex: 0,
    }
}
