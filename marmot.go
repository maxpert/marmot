package main

import (
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/lib"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cleanup := flag.Bool("cleanup", false, "Cleanup all trigger hooks for marmot")
	dbPathString := flag.String("db-path", "/tmp/marmot.db", "Path to SQLite database")
	metaPath := flag.String("raft-path", "/tmp/raft", "Path to save raft information")
	nodeID := flag.Uint64("node-id", rand.Uint64(), "Node ID")
	followFlag := flag.Bool("follow", false, "Start Raft in follower mode")
	shards := flag.Uint64("shards", 16, "Total number of shards for this instance")
	bindAddress := flag.String("bind", "0.0.0.0:8160", "Bind address for Raft server")
	bindPane := flag.String("bind-pane", "localhost:6010", "Bind address for control pane server")
	initialAddrs := flag.String("bootstrap", "", "<CLUSTER_ID>@IP:PORT list of initial nodes separated by comma (,)")
	verbose := flag.Bool("verbose", false, "Log debug level")
	flag.Parse()

	if *verbose {
		log.Logger = log.Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Level(zerolog.InfoLevel)
	}

	log.Debug().Str("path", *dbPathString).Msg("Opening database")
	tableNames, err := db.GetAllDBTables(*dbPathString)
	srcDb, err := db.OpenStreamDB(*dbPathString, tableNames)
	if err != nil {
		log.Error().Err(err).Msg("Unable to open database")
		return
	}

	if *cleanup {
		err = srcDb.RemoveCDC(true)
		if err != nil {
			log.Panic().Err(err).Msg("Unable to clean up...")
		} else {
			log.Info().Msg("Cleanup complete...")
		}

		return
	}

	*metaPath = fmt.Sprintf("%s/node-%d", *metaPath, *nodeID)
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

	srcDb.OnChange = onTableChanged(nodeID, raft)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := srcDb.InstallCDC(); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	err = lib.NewControlPane(raft).Run(*bindPane)
	if err != nil {
		log.Panic().Err(err).Msg("Control pane not working")
	}
}

func onTableChanged(nodeID *uint64, raft *lib.RaftServer) func(event *db.ChangeLogEvent) error {
	return func(event *db.ChangeLogEvent) error {
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
}

func makeRange(min, max uint64) []uint64 {
	a := make([]uint64, max-min+1)
	for i := range a {
		a[i] = min + uint64(i)
	}
	return a
}
