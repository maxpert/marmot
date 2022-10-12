package logstream

import (
	"errors"
	"io"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/marmot/cfg"
)

var ErrNotInitialized = errors.New("not initialized")

type replicationState struct {
	seq  map[string]uint64
	lock *sync.RWMutex
	fl   *os.File
}

func (r *replicationState) init() error {
	fl, err := os.OpenFile(*cfg.SeqMapPath, os.O_RDWR|os.O_CREATE|os.O_SYNC, 0666)
	if err != nil {
		return err
	}

	r.seq = make(map[string]uint64)
	r.lock = &sync.RWMutex{}
	r.fl = fl

	idx, err := fl.Seek(0, io.SeekEnd)
	if idx < 1 {
		return nil
	}

	_, err = fl.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	return cbor.NewDecoder(fl).Decode(&r.seq)
}

func (r *replicationState) save(streamName string, seq uint64) (uint64, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.fl == nil {
		return 0, ErrNotInitialized
	}

	if old, found := r.seq[streamName]; found && seq <= old {
		return old, nil
	}

	_, err := r.fl.Seek(0, io.SeekStart)
	if err != nil {
		return 0, err
	}
	defer r.fl.Sync()

	r.seq[streamName] = seq
	err = cbor.NewEncoder(r.fl).Encode(r.seq)
	if err != nil {
		return 0, err
	}

	return seq, nil
}

func (r *replicationState) get(streamName string) uint64 {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if old, found := r.seq[streamName]; found {
		return old
	}

	return 0
}
