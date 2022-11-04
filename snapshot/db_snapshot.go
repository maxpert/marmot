package snapshot

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"github.com/maxpert/marmot/db"
	"github.com/rs/zerolog/log"
)

var ErrPendingSnapshot = errors.New("system busy capturing snapshot")

const snapshotFileName = "snapshot.db"
const tempDirPattern = "marmot-snapshot-*"

type NatsDBSnapshot struct {
	mutex   *sync.Mutex
	db      *db.SqliteStreamDB
	storage Storage
}

func NewNatsDBSnapshot(d *db.SqliteStreamDB, snapshotStorage Storage) *NatsDBSnapshot {
	return &NatsDBSnapshot{
		mutex:   &sync.Mutex{},
		db:      d,
		storage: snapshotStorage,
	}
}

func (n *NatsDBSnapshot) SaveSnapshot() error {
	locked := n.mutex.TryLock()
	if !locked {
		return ErrPendingSnapshot
	}

	defer n.mutex.Unlock()
	tmpSnapshot, err := os.MkdirTemp(os.TempDir(), tempDirPattern)
	if err != nil {
		return err
	}
	defer cleanupDir(tmpSnapshot)

	bkFilePath := path.Join(tmpSnapshot, snapshotFileName)
	err = n.db.BackupTo(bkFilePath)
	if err != nil {
		return err
	}

	return n.storage.Upload(snapshotFileName, bkFilePath)
}

func (n *NatsDBSnapshot) RestoreSnapshot() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	tmpSnapshotPath, err := os.MkdirTemp(os.TempDir(), tempDirPattern)
	if err != nil {
		return err
	}
	defer cleanupDir(tmpSnapshotPath)

	bkFilePath := path.Join(tmpSnapshotPath, snapshotFileName)
	err = n.storage.Download(bkFilePath, snapshotFileName)
	if err != nil {
		return err
	}

	log.Info().Str("path", bkFilePath).Msg("Downloaded snapshot, restoring...")
	err = db.RestoreFrom(n.db.GetPath(), bkFilePath)
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
		time.Sleep(1 * time.Second)
	}

	log.Error().Str("path", p).Msg("Unable to cleanup temp path, this might cause disk wastage")
}

func fileHash(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := fnv.New64()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return fmt.Sprintf("%x", h.Sum64()), nil
}
