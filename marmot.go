package main

import (
    "errors"
    "flag"
    "fmt"
    "time"

    "github.com/godruoyi/go-snowflake"
    "github.com/mattn/go-sqlite3"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "marmot/db"
    "marmot/lib"
)

func main() {
    cleanup := flag.Bool("cleanup", false, "Cleanup all hooks and tables")
    dbPathString := flag.String("db-path", "/tmp/marmot.db", "Path to SQLITE DB")
    metaPath := flag.String("meta-path", "/tmp/raft", "Path to meta information files")
    nodeID := flag.Uint64("node-id", uint64(snowflake.PrivateIPToMachineID()), "Node ID")
    joinFlag := flag.Bool("join", false, "Join existing cluster")
    clusterID := flag.Uint64("cluster-id", 1, "Cluster ID")
    bindAddress := flag.String("bind", "0.0.0.0:8160", "Bind address for server")
    peersAddrs := flag.String("peers", "", "<CLUSTER_ID>@IP:PORT list of peers separated by comma (,)")
    verbose := flag.Bool("verbose", false, "Log debug level")
    flag.Parse()

    if *verbose {
        log.Logger = log.Level(zerolog.DebugLevel)
    } else {
        log.Logger = log.Level(zerolog.InfoLevel)
    }

    driver := &sqlite3.SQLiteDriver{}
    log.Debug().Msg(fmt.Sprintf("Extensions = %v", driver.Extensions))
    log.Debug().Str("path", *dbPathString).Msg("Opening database")
    srcDb, err := db.OpenSqlite(*dbPathString)
    if err != nil {
        log.Error().Err(err).Msg("Unable to open database")
        return
    }

    if *cleanup {
        err = srcDb.RemoveCDC()
        if err != nil {
            log.Panic().Err(err).Msg("Unable to clean up...")
        } else {
            log.Info().Msg("Cleanup complete...")
        }

        return
    }

    if err := srcDb.InstallCDC(); err != nil {
        log.Error().Err(err).Msg("Unable to install CDC tables")
        return
    }

    nodeHost, err := lib.InitRaft(*bindAddress, *nodeID, *clusterID, *metaPath, srcDb, *peersAddrs, *joinFlag)
    if err != nil {
        log.Panic().Err(err).Msg("Unable to start Raft")
    }

    srcDb.OnChange = func(event *db.ChangeLogEvent) error {
        data, err := event.Marshal()
        if err != nil {
            return err
        }

        session := nodeHost.GetNoOPSession(*clusterID)
        req, err := nodeHost.Propose(session, data, 1*time.Second)
        if err != nil {
            return err
        }

        res := <-req.ResultC()
        if !res.Committed() {
            return errors.New("replication failed")
        }

        return nil
    }

    for {
        time.Sleep(2 * time.Second)
    }
}
