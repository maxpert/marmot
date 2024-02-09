package main

import (
	"context"
	"flag"
	"io"
	"net/http"
	"net/http/pprof"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/maxpert/marmot/telemetry"
	"github.com/maxpert/marmot/utils"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/logstream"
	"github.com/maxpert/marmot/snapshot"

	"github.com/asaskevich/EventBus"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	flag.Parse()
	err := cfg.Load(*cfg.ConfigPathFlag)
	if err != nil {
		panic(err)
	}

	var writer io.Writer = zerolog.NewConsoleWriter()
	if cfg.Config.Logging.Format == "json" {
		writer = os.Stdout
	}
	gLog := zerolog.New(writer).
		With().
		Timestamp().
		Uint64("node_id", cfg.Config.NodeID).
		Logger()

	if cfg.Config.Logging.Verbose {
		log.Logger = gLog.Level(zerolog.DebugLevel)
	} else {
		log.Logger = gLog.Level(zerolog.InfoLevel)
	}

	if *cfg.ProfServer != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

			err := http.ListenAndServe(*cfg.ProfServer, mux)
			if err != nil {
				log.Error().Err(err).Msg("unable to bind profiler server")
			}
		}()
	}

	log.Debug().Msg("Initializing telemetry")
	telemetry.InitializeTelemetry()

	log.Debug().Str("path", cfg.Config.DBPath).Msg("Opening database")
	streamDB, err := db.OpenStreamDB(cfg.Config.DBPath)
	if err != nil {
		log.Error().Err(err).Msg("Unable to open database")
		return
	}

	if *cfg.CleanupFlag {
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

	replicator, err := logstream.NewReplicator(snapshot.NewNatsDBSnapshot(streamDB, snpStore))
	if err != nil {
		log.Panic().Err(err).Msg("Unable to initialize replicators")
	}

	if *cfg.SaveSnapshotFlag {
		replicator.ForceSaveSnapshot()
		return
	}

	if cfg.Config.Snapshot.Enable && cfg.Config.Replicate {
		err = replicator.RestoreSnapshot()
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

	eventBus := EventBus.New()
	ctxSt := utils.NewStateContext()

	streamDB.OnChange = onTableChanged(replicator, ctxSt, eventBus, cfg.Config.NodeID)
	log.Info().Msg("Starting change data capture pipeline...")
	if err := streamDB.InstallCDC(tableNames); err != nil {
		log.Error().Err(err).Msg("Unable to install change data capture pipeline")
		return
	}

	errChan := make(chan error)
	for i := uint64(0); i < cfg.Config.ReplicationLog.Shards; i++ {
		go changeListener(streamDB, replicator, ctxSt, eventBus, i+1, errChan)
	}

	sleepTimeout := utils.AutoResetEventTimer(
		eventBus,
		"pulse",
		time.Duration(cfg.Config.SleepTimeout)*time.Millisecond,
	)
	cleanupInterval := time.Duration(cfg.Config.CleanupInterval) * time.Millisecond
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer cleanupTicker.Stop()

	snapshotInterval := time.Duration(cfg.Config.Snapshot.Interval) * time.Millisecond
	snapshotTicker := utils.NewTimeoutPublisher(snapshotInterval)
	defer snapshotTicker.Stop()

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
		case <-snapshotTicker.Channel():
			if cfg.Config.Snapshot.Enable && cfg.Config.Publish {
				lastSnapshotTime := replicator.LastSaveSnapshotTime()
				now := time.Now()
				if now.Sub(lastSnapshotTime) >= snapshotInterval {
					log.Info().
						Time("last_snapshot", lastSnapshotTime).
						Dur("duration", now.Sub(lastSnapshotTime)).
						Msg("Triggering timer based snapshot save")
					replicator.SaveSnapshot()
				}
			}
		case <-sleepTimeout.Channel():
			log.Info().Msg("No more events to process, initiating shutdown")
			ctxSt.Cancel()
			if cfg.Config.Snapshot.Enable && cfg.Config.Publish {
				log.Info().Msg("Saving snapshot before going to sleep")
				replicator.ForceSaveSnapshot()
			}

			os.Exit(0)
		}
	}
}

func changeListener(
	streamDB *db.SqliteStreamDB,
	rep *logstream.Replicator,
	ctxSt *utils.StateContext,
	events EventBus.BusPublisher,
	shard uint64,
	errChan chan error,
) {
	log.Debug().Uint64("shard", shard).Msg("Listening stream")
	err := rep.Listen(shard, onChangeEvent(streamDB, ctxSt, events))
	if err != nil {
		errChan <- err
	}
}

func onChangeEvent(streamDB *db.SqliteStreamDB, ctxSt *utils.StateContext, events EventBus.BusPublisher) func(data []byte) error {
	return func(data []byte) error {
		events.Publish("pulse")
		if ctxSt.IsCanceled() {
			return context.Canceled
		}

		if !cfg.Config.Replicate {
			return nil
		}

		ev := &logstream.ReplicationEvent[db.ChangeLogEvent]{}
		err := ev.Unmarshal(data)
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}

		return streamDB.Replicate(&ev.Payload)
	}
}

func onTableChanged(r *logstream.Replicator, ctxSt *utils.StateContext, events EventBus.BusPublisher, nodeID uint64) func(event *db.ChangeLogEvent) error {
	return func(event *db.ChangeLogEvent) error {
		events.Publish("pulse")
		if ctxSt.IsCanceled() {
			return context.Canceled
		}

		if !cfg.Config.Publish {
			return nil
		}

		ev := &logstream.ReplicationEvent[db.ChangeLogEvent]{
			FromNodeId: nodeID,
			Payload:    *event,
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
