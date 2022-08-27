package main

import (
    "context"
    "flag"
    "fmt"
    "io"
    "strings"
    "time"

    "github.com/mattn/go-sqlite3"
    "github.com/mitchellh/mapstructure"
    "github.com/rs/zerolog"
    "github.com/rs/zerolog/log"
    "marmot/db"
    "marmot/transport"
)

var peers = make(map[string]*transport.QUICSession, 0)

func main() {
    cleanup := flag.Bool("cleanup", false, "Cleanup all hooks and tables")
    dbPathString := flag.String("db-path", ":memory:", "Path to SQLITE DB")
    bindAddress := flag.String("bind", "0.0.0.0:8160", "Bind address for server")
    peersAddrs := flag.String("peers", "", "IP:PORT list of peers separated by comma (,)")
    verbose := flag.Bool("verbose", false, "Log debug level")
    flag.Parse()

    if *verbose {
        log.Logger = log.Level(zerolog.DebugLevel)
    } else {
        log.Logger = log.Level(zerolog.InfoLevel)
    }

    ctx := context.Background()
    driver := &sqlite3.SQLiteDriver{}

    log.Debug().Msg(fmt.Sprintf("Extensions = %v", driver.Extensions))
    log.Debug().Str("path", *dbPathString).Msg("Opening database")
    srcDb, err := db.OpenSqlite(*dbPathString)
    if err != nil {
        log.Error().Err(err).Msg("Unable to open database")
        return
    }
    defer srcDb.Close()

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

    server, err := transport.NewQUICServer(*bindAddress)
    if err != nil {
        log.Error().Err(err).Msg("Unable to bind")
        return
    }
    defer server.Close()

    if *peersAddrs != "" {
        go joinPeers(ctx, srcDb, *peersAddrs)
    }

    for {
        trans, err := server.Accept(context.Background())
        if err != nil {
            if err != io.EOF {
                log.Warn().Err(err).Msg("End of stream")
            } else {
                log.Debug().Err(err).Msg("Accept error")
            }

            continue
        }

        go loopServe(srcDb, trans)
    }
}

func loopServe(sdb *db.SqliteStreamDB, t *transport.QUICSession) {
    defer t.Close()

    for {
        mappedData, err := t.Read()
        if err != nil {
            if err != io.EOF {
                log.Error().Err(err).Str("pair", t.String()).Msg("End of stream")
            } else {
                log.Debug().Err(err).Msg("Read error")
            }

            return
        }

        data := db.ChangeLogEvent{}
        if err := mapstructure.Decode(mappedData, &data); err != nil {
            log.Error().Err(err).Msg("Invalid payload")
        }

        log.Debug().Msg(fmt.Sprintf("Replicate %s on %s for %d", data.Type, data.Table, data.ChangeRowId))
        err = sdb.Replicate(&data)
        if err != nil {
            msg := fmt.Sprintf("Unable to replicate %s on %s for %d", data.Type, data.Table, data.ChangeRowId)
            log.Error().Err(err).Msg(msg)
        }
    }
}

func joinPeers(ctx context.Context, sdb *db.SqliteStreamDB, peerAddrs string) {
    addrs := strings.Split(peerAddrs, ",")
    sdb.OnChange = func(changes *db.ChangeLogEvent) {
        for addr, p := range peers {
            err := p.Write(changes)
            if err != nil {
                log.Error().
                    Err(err).
                    Int64("id", changes.ChangeRowId).
                    Str("address", addr).
                    Msg("Unable to send")
                p.Close()
                delete(peers, addr)
            }

            log.Debug().
                Int64("id", changes.ChangeRowId).
                Str("node", p.String()).
                Msg("Published to peer")
        }
    }

    for {
        for _, addr := range addrs {
            if _, ok := peers[addr]; ok {
                continue
            }

            c, err := transport.NewQUICClient(addr, ctx)
            if err != nil {
                log.Error().Err(err).Str("address", addr).Msg("Unable to connect")
                continue
            } else {
                log.Info().Str("address", addr).Msg("Connected to peer")
            }

            err = c.Write(nil)

            peers[addr] = c
        }

        time.Sleep(1 * time.Second)
    }
}
