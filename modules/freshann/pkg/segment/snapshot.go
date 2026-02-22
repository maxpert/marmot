package segment

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/maxpert/marmot/modules/freshann/pkg/storage"
)

type SegmentRecord struct {
	ExternalID []byte               `json:"external_id"`
	Record     storage.VectorRecord `json:"record"`
}

func SegmentsDir(indexDir string) string {
	return filepath.Join(indexDir, "segments")
}

func NewSegmentName(ts time.Time) string {
	return ts.UTC().Format("20060102T150405.000000000") + ".seg"
}

func WriteSnapshot(indexDir, name string, records map[string]storage.VectorRecord) error {
	if err := os.MkdirAll(SegmentsDir(indexDir), 0o755); err != nil {
		return err
	}
	path := filepath.Join(SegmentsDir(indexDir), name)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 128*1024)
	for id, rec := range records {
		payload, err := json.Marshal(SegmentRecord{ExternalID: []byte(id), Record: rec})
		if err != nil {
			_ = f.Close()
			return err
		}
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		if _, err := w.Write(lenBuf[:]); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := w.Write(payload); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func ReadSnapshot(indexDir, name string) (map[string]storage.VectorRecord, error) {
	path := filepath.Join(SegmentsDir(indexDir), name)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 128*1024)
	out := make(map[string]storage.VectorRecord)
	for {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		n := binary.BigEndian.Uint32(lenBuf[:])
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return nil, err
		}
		var rec SegmentRecord
		if err := json.Unmarshal(payload, &rec); err != nil {
			return nil, err
		}
		out[string(rec.ExternalID)] = rec.Record
	}
	return out, nil
}
