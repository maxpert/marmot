package stream

import (
	"strings"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats.go"
)

func Connect() (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name(cfg.Config.NodeName()),
		nats.Timeout(10 * time.Second),
	}

	creds, err := getNatsAuthFromConfig()
	if err != nil {
		return nil, err
	}

	opts = append(opts, creds...)
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
