package stream

import (
	"net"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

type embeddedNats struct {
	server *server.Server
	lock   *sync.Mutex
}

var embeddedIns = &embeddedNats{
	server: nil,
	lock:   &sync.Mutex{},
}

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

func startEmbeddedServer(nodeName string) (*embeddedNats, error) {
	embeddedIns.lock.Lock()
	defer embeddedIns.lock.Unlock()

	if embeddedIns.server != nil {
		return embeddedIns, nil
	}

	host, port, err := parseHostAndPort(cfg.Config.NATS.BindAddress)
	if err != nil {
		return nil, err
	}

	opts := &server.Options{
		ServerName:         nodeName,
		Host:               host,
		Port:               port,
		NoSigs:             true,
		JetStream:          true,
		JetStreamMaxMemory: -1,
		JetStreamMaxStore:  -1,
		Cluster: server.ClusterOpts{
			Name: cfg.EmbeddedClusterName,
		},
		LeafNode: server.LeafNodeOpts{},
	}

	if *cfg.ClusterPeersFlag != "" {
		opts.Routes = server.RoutesFromStr(*cfg.ClusterPeersFlag)
	}

	if *cfg.ClusterAddrFlag != "" {
		host, port, err := parseHostAndPort(*cfg.ClusterAddrFlag)
		if err != nil {
			return nil, err
		}

		opts.Cluster.ListenStr = *cfg.ClusterAddrFlag
		opts.Cluster.Host = host
		opts.Cluster.Port = port
	}

	if *cfg.LeafServerFlag != "" {
		opts.LeafNode.Remotes = parseRemoteLeafOpts()
	}

	if cfg.Config.NATS.ServerConfigFile != "" {
		err := opts.ProcessConfigFile(cfg.Config.NATS.ServerConfigFile)
		if err != nil {
			return nil, err
		}
	}

	originalRoutes := opts.Routes
	if len(opts.Routes) != 0 {
		opts.Routes = flattenRoutes(originalRoutes, true)
	}

	if opts.StoreDir == "" {
		opts.StoreDir = path.Join(cfg.DataRootDir, "nats", nodeName)
	}

	s, err := server.NewServer(opts)
	if err != nil {
		return nil, err
	}

	s.SetLogger(
		&natsLogger{log.With().Str("from", "nats").Logger()},
		opts.Debug,
		opts.Trace,
	)
	s.Start()

	embeddedIns.server = s
	return embeddedIns, nil
}

func (e *embeddedNats) prepareConnection(opts ...nats.Option) (*nats.Conn, error) {
	e.lock.Lock()
	s := e.server
	e.lock.Unlock()

	for !s.ReadyForConnections(1 * time.Second) {
		continue
	}

	opts = append(opts, nats.InProcessServer(s))
	for {
		c, err := nats.Connect("", opts...)
		if err != nil {
			log.Warn().Err(err).Msg("NATS server not accepting connections...")
			continue
		}

		j, err := c.JetStream()
		if err != nil {
			return nil, err
		}

		st, err := j.StreamInfo("marmot-r", nats.MaxWait(1*time.Second))
		if err == nats.ErrStreamNotFound || st != nil {
			log.Info().Msg("Streaming ready...")
			return c, nil
		}

		c.Close()
		log.Debug().Err(err).Msg("Streams not ready, waiting for NATS streams to come up...")
		time.Sleep(1 * time.Second)
	}
}
