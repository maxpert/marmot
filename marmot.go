package main

import (
    "errors"
    "flag"
    "strings"
    "time"

    "marmot/db"
    "marmot/lib"

    "github.com/godruoyi/go-snowflake"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
)

func main() {
    cleanup := flag.Bool("cleanup", false, "Cleanup all hooks and tables")
    dbPathString := flag.String("db-path", "/tmp/marmot.db", "Path to SQLITE DB")
    metaPath := flag.String("meta-path", "/tmp/raft", "Path to meta information files")
    nodeID := flag.Uint64("node-id", uint64(snowflake.PrivateIPToMachineID()), "Node ID")
    followFlag := flag.Bool("follow", false, "Start Raft in follower mode")
    shards := flag.Uint64("shards", 16, "Total number of shards for this instance")
    bindAddress := flag.String("bind", "0.0.0.0:8160", "Bind address for Raft server")
    bindPane := flag.String("bind-pane", "localhost:6010", "Bind address for control pane server")
    initialAddrs := flag.String("bootstrap", "", "<CLUSTER_ID>@IP:PORT list of initial nodes separated by comma (,)")
    tables := flag.String("replicate", "", "List of tables to replicate seperated by comma (,)")
    verbose := flag.Bool("verbose", false, "Log debug level")
    flag.Parse()

    if *verbose {
        log.Logger = log.Level(zerolog.DebugLevel)
    } else {
        log.Logger = log.Level(zerolog.InfoLevel)
    }

    log.Debug().Str("path", *dbPathString).Msg("Opening database")
    srcDb, err := db.OpenSqlite(*dbPathString)
    if err != nil {
        log.Error().Err(err).Msg("Unable to open database")
        return
    }

    err = srcDb.RemoveCDC()
    if err != nil {
        log.Panic().Err(err).Msg("Unable to clean up...")
    } else {
        log.Info().Msg("Cleanup complete...")
    }

    if *cleanup {
        return
    }

    raft := lib.NewRaftServer(*bindAddress, *nodeID, *metaPath, srcDb)
    err = raft.Init()
    if err != nil {
        log.Panic().Err(err).Msg("Unable to initialize raft server")
    }

    if !*followFlag {
        clusterIds := makeRange(1, *shards)
        err = raft.BindCluster(*initialAddrs, false, clusterIds...)
        if err != nil {
            log.Panic().Err(err).Msg("Unable to start Raft")
        }
    }

    srcDb.OnChange = func(event *db.ChangeLogEvent) error {
        ev := &lib.ReplicationEvent[db.ChangeLogEvent]{
            FromNodeId: *nodeID,
            Payload:    event,
        }

        data, err := ev.Marshal()
        if err != nil {
            return err
        }

        hash, err := event.Hash()
        if err != nil {
            return err
        }

        res, err := raft.Propose(hash, data, 1*time.Second)
        if err != nil {
            return err
        }

        if !res.Committed() {
            return errors.New("replication failed")
        }

        return nil
    }

    tableNames := strings.Split(*tables, ",")
    if tableNames[0] == "" {
        tableNames = make([]string, 0)
    }
    if err := srcDb.InstallCDC(tableNames); err != nil {
        log.Error().Err(err).Msg("Unable to install CDC tables")
        return
    }

    err = lib.NewControlPane(raft).Run(*bindPane)
    if err != nil {
        log.Panic().Err(err).Msg("Control pane not working")
    }
}

func makeRange(min, max uint64) []uint64 {
    a := make([]uint64, max-min+1)
    for i := range a {
        a[i] = min + uint64(i)
    }
    return a
}
