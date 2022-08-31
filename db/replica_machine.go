package db

import (
    "fmt"
    "io"

    sm "github.com/lni/dragonboat/v3/statemachine"
    "github.com/rs/zerolog/log"
)

type SQLiteStateMachine struct {
    NodeID    uint64
    DB        *SqliteStreamDB
    lastIndex uint64
}

func (ssm *SQLiteStateMachine) Update(bytes []byte) (sm.Result, error) {
    event := &ChangeLogEvent{}
    if err := event.Unmarshal(bytes); err != nil {
        return sm.Result{}, err
    }

    err := ssm.DB.Replicate(event)
    if err != nil {
        return sm.Result{}, err
    }
    
    log.Debug().Msg(fmt.Sprintf("Propagated... %v %v", event.TableName, event.ChangeRowId))
    return sm.Result{Value: 1}, nil
}

func (ssm *SQLiteStateMachine) Lookup(key interface{}) (interface{}, error) {
    return 0, nil
}

func (ssm *SQLiteStateMachine) SaveSnapshot(w io.Writer, fls sm.ISnapshotFileCollection, cancel <-chan struct{}) error {
    return nil
}

func (ssm *SQLiteStateMachine) RecoverFromSnapshot(r io.Reader, fls []sm.SnapshotFile, cancel <-chan struct{}) error {
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
