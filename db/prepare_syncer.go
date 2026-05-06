package db

import (
	"errors"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	prepareSyncBatchSize = 256
	prepareSyncMaxWait   = 5 * time.Millisecond
)

type prepareSyncRequest struct {
	txnID uint64
	done  chan error
}

type prepareSyncer struct {
	store *PebbleMetaStore
	ch    chan *prepareSyncRequest
	done  chan struct{}
	wg    sync.WaitGroup
}

func newPrepareSyncer(store *PebbleMetaStore) *prepareSyncer {
	s := &prepareSyncer{
		store: store,
		ch:    make(chan *prepareSyncRequest, 4096),
		done:  make(chan struct{}),
	}
	s.wg.Add(1)
	go s.loop()
	return s
}

func (s *prepareSyncer) close() {
	if s == nil {
		return
	}
	close(s.done)
	s.wg.Wait()
}

func (s *prepareSyncer) prepare(txnID uint64) error {
	if s == nil {
		return errors.New("prepare syncer is not initialized")
	}
	req := &prepareSyncRequest{
		txnID: txnID,
		done:  make(chan error, 1),
	}
	select {
	case s.ch <- req:
	case <-s.done:
		return errors.New("prepare syncer is closed")
	}
	return <-req.done
}

func (s *prepareSyncer) loop() {
	defer s.wg.Done()
	var batch []*prepareSyncRequest
	timer := time.NewTimer(time.Hour)
	timer.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		err := s.syncBatch(batch)
		for _, req := range batch {
			req.done <- err
		}
		batch = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		if len(batch) == 0 {
			select {
			case req := <-s.ch:
				batch = append(batch, req)
				timer.Reset(prepareSyncMaxWait)
			case <-s.done:
				return
			}
		}

		if len(batch) >= prepareSyncBatchSize {
			flush()
			continue
		}

		select {
		case req := <-s.ch:
			batch = append(batch, req)
		case <-timer.C:
			flush()
		case <-s.done:
			flush()
			return
		}
	}
}

func (s *prepareSyncer) syncBatch(batch []*prepareSyncRequest) error {
	pebbleBatch := s.store.db.NewBatch()
	defer pebbleBatch.Close()

	statusBuf := []byte{byte(TxnStatusPending)}
	for _, req := range batch {
		if err := pebbleBatch.Set(pebbleTxnStatusKey(req.txnID), statusBuf, nil); err != nil {
			return err
		}
	}
	return pebbleBatch.Commit(pebble.Sync)
}
