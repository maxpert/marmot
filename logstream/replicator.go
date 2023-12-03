package logstream

import "time"

type Replicator interface {
	Publish(hash uint64, payload []byte) error
	Listen(shardID uint64, callback func(payload []byte) error) error
	RestoreSnapshot() error
	LastSaveSnapshotTime() time.Time
	SaveSnapshot()
	ForceSaveSnapshot()
}
