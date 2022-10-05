package snapshot

import (
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"
)

var BucketReplicas = 1
var ErrInvalidSnapshot = errors.New("invalid snapshot")
var ErrPendingSnapshot = errors.New("system busy capturing snapshot")
var BucketPrefix = "marmot-snapshot"

const TempDirPattern = "marmot-snapshot-*"
const FileName = "snapshot.db"
const HashHeaderKey = "marmot-snapshot-tag"

type NatsDBSnapshot struct {
	mutex *sync.Mutex
	db    *db.SqliteStreamDB
}

func NewNatsDBSnapshot(d *db.SqliteStreamDB) *NatsDBSnapshot {
	return &NatsDBSnapshot{
		mutex: &sync.Mutex{},
		db:    d,
	}
}

func (n *NatsDBSnapshot) SaveSnapshot(conn *nats.Conn) error {
	locked := n.mutex.TryLock()
	if !locked {
		return ErrPendingSnapshot
	}

	defer n.mutex.Unlock()

	blb, err := getBlobStore(conn)
	if err != nil {
		return err
	}

	tmpSnapshot, err := os.MkdirTemp(os.TempDir(), TempDirPattern)
	if err != nil {
		return err
	}
	defer cleanupDir(tmpSnapshot)

	bkFilePath := path.Join(tmpSnapshot, FileName)
	err = n.db.BackupTo(bkFilePath)
	if err != nil {
		return err
	}

	hash, err := fileHash(bkFilePath)
	if err != nil {
		return err
	}

	rfl, err := os.Open(bkFilePath)
	if err != nil {
		return err
	}
	defer rfl.Close()

	info, err := blb.Put(&nats.ObjectMeta{
		Name: FileName,
		Headers: map[string][]string{
			HashHeaderKey: {hash},
		},
	}, rfl)

	log.Info().
		Str("hash", hash).
		Uint64("size", info.Size).
		Uint32("chunks", info.Chunks).
		Msg("Snapshot saved")

	return nil
}

func (n *NatsDBSnapshot) RestoreSnapshot(conn *nats.Conn) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	blb, err := getBlobStore(conn)
	if err != nil {
		return err
	}

	tmpSnapshot, err := os.MkdirTemp(os.TempDir(), TempDirPattern)
	if err != nil {
		return err
	}
	defer cleanupDir(tmpSnapshot)

	info, err := blb.GetInfo(FileName)
	if err == nats.ErrObjectNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	snapshotHash, ok := info.Headers[HashHeaderKey]
	if !ok || len(snapshotHash) != 1 {
		return ErrInvalidSnapshot
	}

	bkFilePath := path.Join(tmpSnapshot, FileName)
	err = n.db.BackupTo(bkFilePath)
	if err != nil {
		return err
	}

	hash, err := fileHash(bkFilePath)
	if err != nil {
		return err
	}

	if hash == snapshotHash[0] {
		log.Info().Msg("DB Snapshot already up to date, skipping restore")
		return nil
	}

	err = blb.GetFile(FileName, bkFilePath)
	if err != nil {
		return err
	}

	log.Info().Str("path", bkFilePath).Msg("Downloaded snapshot, restoring...")
	err = n.db.RestoreFrom(bkFilePath)
	if err != nil {
		return err
	}

	log.Info().Str("path", bkFilePath).Msg("Restore complete...")
	return nil
}

func cleanupDir(p string) {
	for i := 0; i < 5; i++ {
		err := os.RemoveAll(p)
		if err == nil {
			return
		}

		log.Warn().Err(err).Str("path", p).Msg("Unable to cleanup directory path")
		time.Sleep(2 * time.Second)
	}

	log.Error().Str("path", p).Msg("Unable to cleanup temp path, this might cause disk wastage")
}

func fileHash(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := crc64.New(crc64.MakeTable(crc64.ECMA))
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum64()), nil
}

func getBlobStore(conn *nats.Conn) (nats.ObjectStore, error) {
	js, err := conn.JetStream()
	if err != nil {
		return nil, err
	}

	blb, err := js.ObjectStore(blobBucketName())
	if err == nats.ErrStreamNotFound {
		blb, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
			Bucket:      keyValueBucketName(),
			Replicas:    BucketReplicas,
			Storage:     nats.FileStorage,
			Description: "Bucket to store snapshot",
		})
	}

	return blb, err
}

func keyValueBucketName() string {
	return BucketPrefix + "-meta"
}

func blobBucketName() string {
	return BucketPrefix + "-blob"
}
