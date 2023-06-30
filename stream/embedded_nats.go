package stream

import (
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats-server/v2/logger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

var embeddedServer *server.Server = nil
var embeddedLock = &sync.Mutex{}

func parseHostAndPort(adr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(adr)
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
		JetStreamMaxMemory: 1 << 25,
		JetStreamMaxStore:  1 << 30,
		Cluster: server.ClusterOpts{
			Name: "e-marmot",
		},
	}

	if *cfg.ClusterPeers != "" {
		opts.Routes = server.RoutesFromStr(*cfg.ClusterPeers)
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

	if opts.StoreDir == "" {
		opts.StoreDir = path.Join(cfg.TmpDir, "nats", nodeName)
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	s.SetLogger(
		logger.NewStdLogger(true, opts.Debug, opts.Trace, true, false),
		opts.Debug,
		opts.Trace,
	)
	s.Start()

	err = touchStreamReady(nodeName, s)
	if err != nil {
		return nil, err
	}

	embeddedServer = s
	return s, nil
}

func touchStreamReady(nodeName string, s *server.Server) error {
	for {
		c, err := nats.Connect("", nats.InProcessServer(s))
		if err != nil {
			return err
		}

		j, err := c.JetStream()
		if err != nil {
			return err
		}

		st, err := j.StreamInfo(nodeName, nats.MaxWait(1*time.Second))
		if err == nats.ErrStreamNotFound || st != nil {
			log.Info().Msg("Streaming ready...")
			return nil
		}

		c.Close()
		log.Info().Err(err).Msg("Streams not ready, waiting for NATS streams to come up...")
		time.Sleep(1 * time.Second)
	}
}
