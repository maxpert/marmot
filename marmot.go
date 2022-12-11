package main

import (
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/logstream"
	"github.com/maxpert/marmot/snapshot"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	flag.Parse()

	err := cfg.Load(*cfg.ConfigPath)
	if err != nil {
		panic(err)
	}

	var writer io.Writer = zerolog.NewConsoleWriter()
	if cfg.Config.Logging.Format == "json" {
		writer = os.Stdout
	}
	gLog := zerolog.New(writer).With().Timestamp().Logger()

	if cfg.Config.Logging.Verbose {
		log.Logger = gLog.Level(zerolog.DebugLevel)
	} else {
		log.Logger = gLog.Level(zerolog.InfoLevel)
	}

	log.Debug().Str("path", cfg.Config.DBPath).Msg("Opening database")
	streamDB, err := db.OpenStreamDB(cfg.Config.DBPath)
	if err != nil {
		log.Error().Err(err).Msg("Unable to open database")
		return
	}

	if *cfg.Cleanup {
		err = streamDB.RemoveCDC(true)
		if err != nil {
			log.Panic().Err(err).Msg("Unable to clean up...")
		} else {
			log.Info().Msg("Cleanup complete...")
		}

		return
	}

	snpStore, err := snapshot.NewSnapshotStorage()
	if err != nil {
		log.Panic().Err(err).Msg("Unable to initialize snapshot storage")
	}

	rep, err := logstream.NewReplicator(
		cfg.Config.NodeID,
		strings.Join(cfg.Config.NATS.URLs, ", "),
		cfg.Config.ReplicationLog.Shards,
		cfg.Config.ReplicationLog.Compress,
		snapshot.NewNatsDBSnapshot(streamDB, snpStore),
	)
	if err != nil {
		log.Panic().Err(err).Msg("Unable to initialize replicators")
	}

	if *cfg.SaveSnapshot {
		rep.SaveSnapshot()
		return
	}

	if cfg.Config.Snapshot.Enable && cfg.Config.Replicate {
		err = rep.RestoreSnapshot()
		if err != nil {
			log.Panic().Err(err).Msg("Unable to restore snapshot")
		}
	}

	log.Info().Msg("Listing tables to watch...")
	tableNames, err := db.GetAllDBTables(cfg.Config.DBPath)
	if err != nil {
		log.Error().Err(err).Msg("Unable to list all tables")
		return
	}

	streamDB.OnChange = onTableChanged(rep, cfg.Config.NodeID)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := streamDB.InstallCDC(tableNames); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	errChan := make(chan error)
	for i := uint64(0); i < cfg.Config.ReplicationLog.Shards; i++ {
		go changeListener(streamDB, rep, i+1, errChan)
	}

	cleanupInterval := time.Duration(cfg.Config.CleanInterval) * time.Second
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
		if !cfg.Config.Replicate {
			return nil
		}

		ev := &logstream.ReplicationEvent[db.ChangeLogEvent]{}
		err := ev.Unmarshal(data)
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}

		return streamDB.Replicate(ev.Payload)
	}
}

func onTableChanged(r *logstream.Replicator, nodeID uint64) func(event *db.ChangeLogEvent) error {
	return func(event *db.ChangeLogEvent) error {
		if !cfg.Config.Publish {
			return nil
		}

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
