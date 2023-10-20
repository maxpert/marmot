package stream

import (
	"fmt"
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

func Connect() (*nats.Conn, error) {
	opts := setupConnOptions()

	creds, err := getNatsAuthFromConfig()
	if err != nil {
		return nil, err
	}

	tls, err := getNatsTLSFromConfig()
	if err != nil {
		return nil, err
	}

	opts = append(opts, creds...)
	opts = append(opts, tls...)
	if len(cfg.Config.NATS.URLs) == 0 {
		embedded, err := startEmbeddedServer(cfg.Config.NodeName())
		if err != nil {
			return nil, err
		}

		return embedded.prepareConnection(opts...)
	}

	return nats.Connect(
		strings.Join(cfg.Config.NATS.URLs, ", "),
		opts...,
	)
}

func getNatsAuthFromConfig() ([]nats.Option, error) {
	opts := make([]nats.Option, 0)

	if cfg.Config.NATS.CredsUser != "" {
		opt := nats.UserInfo(cfg.Config.NATS.CredsUser, cfg.Config.NATS.CredsPassword)
		opts = append(opts, opt)
	}

	if cfg.Config.NATS.SeedFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.Config.NATS.SeedFile)
		if err != nil {
			return nil, err
		}

		opts = append(opts, opt)
	}

	return opts, nil
}

func getNatsTLSFromConfig() ([]nats.Option, error) {
	opts := make([]nats.Option, 0)

	if cfg.Config.NATS.CAFile != "" {
		opt := nats.RootCAs(cfg.Config.NATS.CAFile)
		opts = append(opts, opt)
	}

	if cfg.Config.NATS.CertFile != "" && cfg.Config.NATS.KeyFile != "" {
		opt := nats.ClientCert(cfg.Config.NATS.CertFile, cfg.Config.NATS.KeyFile)
		opts = append(opts, opt)
	}

	return opts, nil
}

func setupConnOptions() []nats.Option {
	opts := []nats.Option{
		nats.Name(cfg.Config.NodeName()),
		nats.Timeout(10 * time.Second),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Fatal().Msg(fmt.Sprintf("Exiting: %v", nc.LastError()))
		}),
	}
	if cfg.Config.NATS.ConnectTotalWaitMinutes == 0 {
		return opts
	}

	totalWait := time.Duration(cfg.Config.NATS.ConnectTotalWaitMinutes) * time.Minute
	reconnectDelay := time.Second

	opts = append(opts, nats.RetryOnFailedConnect(true))
	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("Disconnected due to: %v, will attempt reconnects for %.0fm", err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("Reconnected to [%s]", nc.ConnectedUrl())
	}))

	return opts
}
