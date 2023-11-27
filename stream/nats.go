package stream

import (
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

	url := strings.Join(cfg.Config.NATS.URLs, ", ")

	var conn *nats.Conn
	for i := 0; i < cfg.Config.NATS.ConnectRetries; i++ {
		conn, err = nats.Connect(url, opts...)
		if err == nil && conn.Status() == nats.CONNECTED {
			break
		}

		log.Warn().
			Err(err).
			Int("attempt", i+1).
			Int("attempt_limit", cfg.Config.NATS.ConnectRetries).
			Str("status", conn.Status().String()).
			Msg("NATS connection failed")
	}

	return conn, err
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
	return []nats.Option{
		nats.Name(cfg.Config.NodeName()),
		nats.RetryOnFailedConnect(true),
		nats.ReconnectWait(time.Duration(cfg.Config.NATS.ReconnectWaitSeconds) * time.Second),
		nats.MaxReconnects(cfg.Config.NATS.ConnectRetries),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Error().
				Err(nc.LastError()).
				Msg("NATS client exiting")
		}),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			log.Error().
				Err(err).
				Msg("NATS client disconnected")
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Info().
				Str("url", nc.ConnectedUrl()).
				Msg("NATS client reconnected")
		}),
	}
}
