package db

import (
    "fmt"
    "io"

    "github.com/doug-martin/goqu/v9"
    sm "github.com/lni/dragonboat/v3/statemachine"
    "github.com/rs/zerolog/log"
)

type SQLiteStateMachine struct {
    ShardID   uint64
    ReplicaID uint64
    DB        *goqu.Database
    lastIndex uint64
}

func (S *SQLiteStateMachine) Update(bytes []byte) (sm.Result, error) {
    event := &ChangeLogEvent{}
    if err := event.Unmarshal(bytes); err != nil {
        return sm.Result{}, err
    }

    log.Debug().Msg(fmt.Sprintf("Propagated... %v %v", event.TableName, event.ChangeRowId))
    return sm.Result{Value: 1}, nil
}

func (S *SQLiteStateMachine) Lookup(key interface{}) (interface{}, error) {
    log.Debug().Msg("Lookup")
    return 0, nil
}

func (S *SQLiteStateMachine) SaveSnapshot(w io.Writer, fls sm.ISnapshotFileCollection, cancel <-chan struct{}) error {
    return nil
}

func (S *SQLiteStateMachine) RecoverFromSnapshot(r io.Reader, fls []sm.SnapshotFile, cancel <-chan struct{}) error {
    return nil
}

func (S *SQLiteStateMachine) Close() error {
    return nil
}

func NewDBStateMachine(shardID uint64, replicaID uint64, db *goqu.Database) sm.IStateMachine {
    return &SQLiteStateMachine{
        DB:        db,
        ShardID:   shardID,
        ReplicaID: replicaID,
        lastIndex: 0,
    }
}
