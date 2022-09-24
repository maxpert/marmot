package main

import (
	"flag"
	"math/rand"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/lib"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cleanup := flag.Bool("cleanup", false, "Cleanup all trigger hooks for marmot")
	dbPathString := flag.String("db-path", "/tmp/marmot.db", "Path to SQLite database")
	nodeID := flag.Uint64("node-id", rand.Uint64(), "Node ID")
	natsAddr := flag.String("nats-url", lib.DefaultUrl, "NATS server URL")
	shards := flag.Uint64("shards", 512, "Number of stream shards to distribute change log on")
	verbose := flag.Bool("verbose", false, "Log debug level")
	flag.Parse()

	if *verbose {
		log.Logger = log.Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Level(zerolog.InfoLevel)
	}

	log.Debug().Str("path", *dbPathString).Msg("Opening database")
	tableNames, err := db.GetAllDBTables(*dbPathString)
	streamDB, err := db.OpenStreamDB(*dbPathString, tableNames)
	if err != nil {
		log.Error().Err(err).Msg("Unable to open database")
		return
	}

	if *cleanup {
		err = streamDB.RemoveCDC(true)
		if err != nil {
			log.Panic().Err(err).Msg("Unable to clean up...")
		} else {
			log.Info().Msg("Cleanup complete...")
		}

		return
	}

	rep, err := lib.NewReplicator(*nodeID, *natsAddr, *shards)
	if err != nil {
		log.Panic().Err(err).Msg("Unable to connect")
	}

	streamDB.OnChange = onTableChanged(rep, *nodeID, *shards)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := streamDB.InstallCDC(); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	err = rep.Listen(onChangeEvent(streamDB))
	if err != nil {
		log.Panic().Err(err).Msg("Listener terminated")
	}
}

func onChangeEvent(streamDB *db.SqliteStreamDB) func(data []byte) error {
	return func(data []byte) error {
		ev := &lib.ReplicationEvent[db.ChangeLogEvent]{}
		err := ev.Unmarshal(data)
		if err != nil {
			return err
		}

		return streamDB.Replicate(ev.Payload)
	}
}

func onTableChanged(r *lib.Replicator, nodeID uint64, shards uint64) func(event *db.ChangeLogEvent) error {
	return func(event *db.ChangeLogEvent) error {
		ev := &lib.ReplicationEvent[db.ChangeLogEvent]{
			FromNodeId: nodeID,
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

		err = r.Publish(hash%shards, data)
		if err != nil {
			return err
		}

		return nil
	}
}
