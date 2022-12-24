package snapshot

import (
	"errors"

	"github.com/maxpert/marmot/cfg"
)

var ErrInvalidStorageType = errors.New("invalid snapshot storage type")
var ErrNoSnapshotFound = errors.New("no snapshot found")

type NatsSnapshot interface {
	SaveSnapshot() error
	RestoreSnapshot() error
}

type Storage interface {
	Upload(name, filePath string) error
	Download(filePath, name string) error
}

func NewSnapshotStorage() (Storage, error) {
	c := cfg.Config

	switch c.SnapshotStorageType() {
	case cfg.Nats:
		return newNatsStorage()
	case cfg.S3:
		return newS3Storage()
	}

	return nil, ErrInvalidStorageType
}
