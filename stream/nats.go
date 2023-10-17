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
	opts := []nats.Option{
		nats.Name(cfg.Config.NodeName()),
		nats.Timeout(time.Duration(cfg.Config.NATS.ConnectTimeoutSeconds) * time.Second),
	}

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
		if err == nil {
			break
		}

		log.Warn().Err(err).Msg(fmt.Sprintf(
			"NATS connection attempt %d/%d failed", i+1, cfg.Config.NATS.ConnectRetries,
		))

		if cfg.Config.NATS.ConnectRetryDelaySeconds > 0 {
			log.Warn().Msg(fmt.Sprintf(
				"Retrying in %d seconds...", cfg.Config.NATS.ConnectRetries,
			))
			time.Sleep(time.Duration(cfg.Config.NATS.ConnectRetryDelaySeconds))
		}

		continue
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
