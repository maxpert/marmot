package grpc

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const loadDataStageTTL = 10 * time.Minute

type stagedLoadData struct {
	data      []byte
	expiresAt time.Time
}

var loadDataStage sync.Map // key: load_id (string), value: stagedLoadData

func stageLoadDataPayload(loadID string, data []byte) {
	if loadID == "" || len(data) == 0 {
		return
	}
	cp := append([]byte(nil), data...)
	loadDataStage.Store(loadID, stagedLoadData{
		data:      cp,
		expiresAt: time.Now().Add(loadDataStageTTL),
	})
}

func getLoadDataChunk(loadID string, offset uint64, maxBytes uint32) ([]byte, uint64, bool) {
	v, ok := loadDataStage.Load(loadID)
	if !ok {
		return nil, 0, false
	}
	entry := v.(stagedLoadData)
	if time.Now().After(entry.expiresAt) {
		loadDataStage.Delete(loadID)
		return nil, 0, false
	}

	total := uint64(len(entry.data))
	if offset >= total {
		return []byte{}, total, true
	}

	end := offset + uint64(maxBytes)
	if maxBytes == 0 || end > total {
		end = total
	}
	chunk := append([]byte(nil), entry.data[offset:end]...)
	return chunk, total, true
}

func cleanupStagedLoadDataByTxn(txnID uint64) {
	prefix := fmt.Sprintf("%d:", txnID)
	loadDataStage.Range(func(key, _ interface{}) bool {
		k, ok := key.(string)
		if ok && strings.HasPrefix(k, prefix) {
			loadDataStage.Delete(k)
		}
		return true
	})
}
