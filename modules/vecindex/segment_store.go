package vecindex

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	SegmentStoreVersion = 1

	segmentManifestMagic = "MVSMAN01"
	segmentCurrentMagic  = "MVSCUR01"
)

type SegmentManifest struct {
	Version           uint32   `msgpack:"version"`
	Database          string   `msgpack:"database"`
	IndexName         string   `msgpack:"index_name"`
	IndexCreatedAt    int64    `msgpack:"index_created_at"`
	Metric            string   `msgpack:"metric"`
	Dim               uint32   `msgpack:"dim"`
	InternalDim       uint32   `msgpack:"internal_dim"`
	CentroidEpoch     uint64   `msgpack:"centroid_epoch"`
	CentroidBlob      []byte   `msgpack:"centroid_blob"`
	AppliedOverlaySeq uint64   `msgpack:"applied_overlay_seq"`
	Generation        uint64   `msgpack:"generation"`
	DataFile          string   `msgpack:"data_file"`
	DataFileSize      uint64   `msgpack:"data_file_size"`
	DataFileSHA256    string   `msgpack:"data_file_sha256"`
	RowMapFile        string   `msgpack:"rowmap_file"`
	RowMapFileSize    uint64   `msgpack:"rowmap_file_size"`
	RowMapFileSHA256  string   `msgpack:"rowmap_file_sha256"`
	MaxCluster        uint32   `msgpack:"max_cluster"`
	RowCount          uint64   `msgpack:"row_count"`
	LayoutHotClusters []uint32 `msgpack:"layout_hot_clusters,omitempty"`
	CreatedAtUnixNano int64    `msgpack:"created_at_unix_nano"`
}

type SegmentCurrent struct {
	Version      uint32 `msgpack:"version"`
	Generation   uint64 `msgpack:"generation"`
	ManifestFile string `msgpack:"manifest_file"`
}

func SegmentStoreDir(dbPath, indexName string) string {
	return dbPath + "." + indexName + ".vecseg"
}

func SegmentCurrentPath(dir string) string {
	return filepath.Join(dir, "manifest", "current")
}

func SegmentManifestPath(dir string, generation uint64) string {
	return filepath.Join(dir, "manifest", fmt.Sprintf("gen-%020d.mf", generation))
}

func SegmentDataPath(dir string, generation uint64) string {
	return filepath.Join(dir, "segments", fmt.Sprintf("gen-%020d.dat", generation))
}

func SegmentRowMapPath(dir string, generation uint64) string {
	return filepath.Join(dir, "rowmap", fmt.Sprintf("gen-%020d.rmap", generation))
}

func OverlayJournalPath(dir string) string {
	return filepath.Join(dir, "overlay", "current.log")
}

func EncodeSegmentManifest(manifest *SegmentManifest) ([]byte, error) {
	return encodeSegmentEnvelope(segmentManifestMagic, manifest)
}

func DecodeSegmentManifest(data []byte) (*SegmentManifest, error) {
	var manifest SegmentManifest
	if err := decodeSegmentEnvelope(segmentManifestMagic, data, &manifest); err != nil {
		return nil, err
	}
	return &manifest, nil
}

func EncodeSegmentCurrent(current *SegmentCurrent) ([]byte, error) {
	return encodeSegmentEnvelope(segmentCurrentMagic, current)
}

func DecodeSegmentCurrent(data []byte) (*SegmentCurrent, error) {
	var current SegmentCurrent
	if err := decodeSegmentEnvelope(segmentCurrentMagic, data, &current); err != nil {
		return nil, err
	}
	return &current, nil
}

func encodeSegmentEnvelope(magic string, v any) ([]byte, error) {
	payload, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, 12+len(payload))
	out = append(out, []byte(magic)...)
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(payload))
	out = append(out, checksum[:]...)
	out = append(out, payload...)
	return out, nil
}

func decodeSegmentEnvelope(magic string, data []byte, dst any) error {
	if len(data) < 12 {
		return fmt.Errorf("vecindex: short segment envelope")
	}
	if string(data[:8]) != magic {
		return fmt.Errorf("vecindex: invalid segment envelope magic")
	}
	payload := data[12:]
	want := binary.LittleEndian.Uint32(data[8:12])
	if got := crc32.ChecksumIEEE(payload); got != want {
		return fmt.Errorf("vecindex: segment envelope checksum mismatch")
	}
	if err := msgpack.Unmarshal(payload, dst); err != nil {
		return fmt.Errorf("vecindex: decode segment envelope: %w", err)
	}
	return nil
}
