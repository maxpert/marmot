package snapshot

import (
	"os"
	"strings"

	"github.com/maxpert/marmot/cfg"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

const hashHeaderKey = "marmot-snapshot-tag"

type natsStorage struct {
	nc *nats.Conn
}

func (n *natsStorage) Upload(name, filePath string) error {
	blb, err := getBlobStore(n.nc)
	if err != nil {
		return err
	}

	err = blb.Delete(name)
	if err != nil && err != nats.ErrObjectNotFound {
		return err
	}

	hash, err := fileHash(filePath)
	if err != nil {
		return err
	}

	rfl, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer rfl.Close()

	info, err := blb.Put(&nats.ObjectMeta{
		Name: name,
		Headers: map[string][]string{
			hashHeaderKey: {hash},
		},
	}, rfl)
	if err != nil {
		return err
	}

	log.Info().
		Str("hash", hash).
		Str("file_name", name).
		Uint64("size", info.Size).
		Str("file_path", filePath).
		Uint32("chunks", info.Chunks).
		Msg("Snapshot saved to NATS")

	return nil
}

func (n *natsStorage) Download(filePath, name string) error {
	blb, err := getBlobStore(n.nc)
	if err != nil {
		return err
	}

	err = blb.GetFile(name, filePath)
	if err == nats.ErrObjectNotFound {
		return ErrNoSnapshotFound
	}

	return err
}

func newNatsStorage() (*natsStorage, error) {
	c := cfg.Config
	urls := strings.Join(c.NATS.URLs, ", ")
	nc, err := nats.Connect(urls, nats.Name(c.NodeName()))
	if err != nil {
		return nil, err
	}

	return &natsStorage{nc: nc}, nil
}

func getBlobStore(conn *nats.Conn) (nats.ObjectStore, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	blb, err := js.ObjectStore(blobBucketName())
	if err == nats.ErrStreamNotFound {
		blb, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
			Bucket:      blobBucketName(),
			Replicas:    cfg.Config.Snapshot.Nats.Replicas,
			Storage:     nats.FileStorage,
			Description: "Bucket to store snapshot",
		})
	}

	return blb, err
}

func blobBucketName() string {
	if cfg.Config.Snapshot.Nats.BucketName == "" {
		return cfg.Config.NATS.StreamPrefix + "-snapshot-store"
	}

	return cfg.Config.Snapshot.Nats.BucketName
}
