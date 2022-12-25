package stream

import (
	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/rs/zerolog/log"
	"net"
	"os"
	"path"
	"strconv"
	"sync"
)

var embeddedServer *server.Server = nil
var embeddedLock = &sync.Mutex{}

func parseHostAndPort(adr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(*cfg.ClusterListenAddr)
	if err != nil {
		return "", 0, err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return "", 0, err
	}

	return host, port, nil
}

func startEmbeddedServer(nodeName string) (*server.Server, error) {
	embeddedLock.Lock()
	defer embeddedLock.Unlock()

	if embeddedServer != nil {
		return embeddedServer, nil
	}

	opts := &server.Options{
		ServerName:         nodeName,
		Host:               "127.0.0.1",
		Port:               -1,
		NoSigs:             true,
		JetStream:          true,
		StoreDir:           path.Join(os.TempDir(), "nats", nodeName),
		JetStreamMaxMemory: 1 << 25,
		JetStreamMaxStore:  1 << 30,
		Routes:             server.RoutesFromStr(*cfg.ClusterPeers),
		Cluster: server.ClusterOpts{
			Name: "e-marmot",
		},
	}

	if *cfg.ClusterListenAddr != "" {
		host, port, err := parseHostAndPort(*cfg.ClusterListenAddr)
		if err != nil {
			return nil, err
		}

		opts.Cluster.ListenStr = *cfg.ClusterListenAddr
		opts.Cluster.Host = host
		opts.Cluster.Port = port
	}

	if cfg.Config.NATS.ServerConfigFile != "" {
		err := opts.ProcessConfigFile(cfg.Config.NATS.ServerConfigFile)
		if err != nil {
			return nil, err
		}
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	s.SetLogger(logger.NewStdLogger(true, false, false, true, false), true, false)
	s.Start()
	log.Info().
		Bool("clustered", s.JetStreamIsClustered()).
		Msg("Started embedded JetStream server...")
	embeddedServer = s
	return s, nil
}
