package main

import (
	"flag"
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

	if *cfg.Verbose {
		log.Logger = log.Level(zerolog.DebugLevel)
	} else {
		log.Logger = log.Level(zerolog.InfoLevel)
	}

	log.Debug().Str("path", *cfg.DBPathString).Msg("Opening database")
	streamDB, err := db.OpenStreamDB(*cfg.DBPathString)
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

	rep, err := logstream.NewReplicator(
		*cfg.NodeID,
		*cfg.NatsAddr,
		*cfg.Shards,
		*cfg.EnableCompress,
		snapshot.NewNatsDBSnapshot(streamDB),
	)
	if err != nil {
		log.Panic().Err(err).Msg("Unable to connect")
	}

	if *cfg.SaveSnapshot {
		rep.SaveSnapshot()
		return
	}

	if *cfg.EnableSnapshot {
		err = rep.RestoreSnapshot()
		if err != nil {
			log.Panic().Err(err).Msg("Unable to restore snapshot")
		}
	}

	log.Info().Msg("Listing tables to watch...")
	tableNames, err := db.GetAllDBTables(*cfg.DBPathString)
	if err != nil {
		log.Error().Err(err).Msg("Unable to list all tables")
		return
	}

	streamDB.OnChange = onTableChanged(rep, *cfg.NodeID)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := streamDB.InstallCDC(tableNames); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	errChan := make(chan error)
	for i := uint64(0); i < *cfg.Shards; i++ {
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
