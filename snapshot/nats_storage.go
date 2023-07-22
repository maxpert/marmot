package snapshot

import (
	"os"
	"time"

	"github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/stream"
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
		Str("file_path", filePath).
		Uint64("size", info.Size).
		Uint32("chunks", info.Chunks).
		Msg("Snapshot saved to NATS")

	return nil
}

func (n *natsStorage) Download(filePath, name string) error {
	blb, err := getBlobStore(n.nc)
	if err != nil {
		return err
	}

	for {
		err = blb.GetFile(name, filePath)
		if err == nil {
			return nil
		}

		if err == nats.ErrObjectNotFound {
			return ErrNoSnapshotFound
		}

		if jsmErr, ok := err.(nats.JetStreamError); ok {
			log.Warn().
				Err(err).
				Int("Status", jsmErr.APIError().Code).
				Msg("Error downloading snapshot")

			if jsmErr.APIError().Code == 503 {
				time.Sleep(time.Second)
				continue
			}
		}

		return err
	}
}

func getBlobStore(conn *nats.Conn) (nats.ObjectStore, error) {
	js, err := conn.JetStream(nats.MaxWait(30 * time.Second))
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

func newNatsStorage() (*natsStorage, error) {
	nc, err := stream.Connect()
	if err != nil {
		return nil, err
	}

	return &natsStorage{nc: nc}, nil
}

func blobBucketName() string {
	if cfg.Config.Snapshot.Nats.BucketName == "" {
		return cfg.Config.NATS.StreamPrefix + "-snapshot-store"
	}

	return cfg.Config.Snapshot.Nats.BucketName
}
