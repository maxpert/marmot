package main

import (
	"flag"
	"math/rand"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/logstream"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	cleanup := flag.Bool("cleanup", false, "Cleanup all trigger hooks for marmot")
	dbPathString := flag.String("db-path", "/tmp/marmot.db", "Path to SQLite database")
	nodeID := flag.Uint64("node-id", rand.Uint64(), "Node ID")
	natsAddr := flag.String("nats-url", logstream.DefaultUrl, "NATS server URL")
	shards := flag.Uint64("shards", 8, "Number of stream shards to distribute change log on")
	maxLogEntries := flag.Int64("max-log-entries", 1024, "Maximum number of change log entries to persist")
	logReplicas := flag.Int("log-replicas", 1, "Number of copies to be committed for single change log")
	subjectPrefix := flag.String("subject-prefix", "marmot-change-log", "Prefix for publish subjects")
	streamPrefix := flag.String("stream-prefix", "marmot-changes", "Prefix for publish subjects")
	verbose := flag.Bool("verbose", false, "Log debug level")
	flag.Parse()

	logstream.MaxLogEntries = *maxLogEntries
	logstream.EntryReplicas = *logReplicas
	logstream.SubjectPrefix = *subjectPrefix
	logstream.StreamNamePrefix = *streamPrefix

	if *verbose {
		log.Logger = log.Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Level(zerolog.InfoLevel)
	}

	log.Debug().Str("path", *dbPathString).Msg("Opening database")
	tableNames, err := db.GetAllDBTables(*dbPathString)
	if err != nil {
		log.Error().Err(err).Msg("Unable to list all tables")
		return
	}

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

	rep, err := logstream.NewReplicator(*nodeID, *natsAddr, *shards)
	if err != nil {
		log.Panic().Err(err).Msg("Unable to connect")
	}

	streamDB.OnChange = onTableChanged(rep, *nodeID)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := streamDB.InstallCDC(); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	errChan := make(chan error)
	for i := uint64(0); i < *shards; i++ {
		go changeListener(streamDB, rep, i+1, errChan)
	}

	cleanupInterval := 5 * time.Second
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()

	for {
		select {
		case err = <-errChan:
			if err != nil {
				log.Panic().Err(err).Msg("Terminated listener")
			}
		case t := <-cleanupTicker.C:
			cnt, err := streamDB.CleanupChangeLogs(t.Add(-cleanupInterval))
			if err != nil {
				log.Warn().Err(err).Msg("Unable to cleanup change logs")
			} else if cnt > 0 {
				log.Debug().Int64("count", cnt).Msg("Cleaned up DB change logs")
			}
		}
	}
}

func changeListener(streamDB *db.SqliteStreamDB, rep *logstream.Replicator, shard uint64, errChan chan error) {
	log.Debug().Uint64("shard", shard).Msg("Listening stream")
	err := rep.Listen(shard, onChangeEvent(streamDB))
	if err != nil {
		errChan <- err
	}
}

func onChangeEvent(streamDB *db.SqliteStreamDB) func(data []byte) error {
	return func(data []byte) error {
		ev := &logstream.ReplicationEvent[db.ChangeLogEvent]{}
		err := ev.Unmarshal(data)
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}

		_, _ = streamDB.DeleteChangeLog(ev.Payload)
		return streamDB.Replicate(ev.Payload)
	}
}

func onTableChanged(r *logstream.Replicator, nodeID uint64) func(event *db.ChangeLogEvent) error {
	return func(event *db.ChangeLogEvent) error {
		ev := &logstream.ReplicationEvent[db.ChangeLogEvent]{
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

		err = r.Publish(hash, data)
		if err != nil {
			return err
		}

		return nil
	}
}
