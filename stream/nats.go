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

	serverUrl := strings.Join(cfg.Config.NATS.URLs, ", ")
	if serverUrl == "" {
		server, err := startEmbeddedServer(cfg.Config.NodeName())
		if err != nil {
			return nil, err
		}

		opts = append(opts, nats.InProcessServer(server))
	}

	if cfg.Config.NATS.SeedFile != "" {
		opt, err := nats.NkeyOptionFromSeed(cfg.Config.NATS.SeedFile)
		if err != nil {
			return nil, err
		}

		opts = append(opts, opt)
	}

	nc, err := nats.Connect(serverUrl, opts...)
	if err != nil {
		return nil, err
	}

	return nc, nil
}
