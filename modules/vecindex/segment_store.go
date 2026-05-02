package vecindex

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"

	"github.com/vmihailenco/msgpack/v5"
)

const (
	SegmentStoreVersion = 4
	segmentStoreV3      = 3
	segmentStoreV2      = 2
	segmentStoreV1      = 1

	segmentManifestMagic = "MVSMAN01"
	segmentCurrentMagic  = "MVSCUR01"
)

type SegmentManifest struct {
	Version                  uint32      `msgpack:"version"`
	Database                 string      `msgpack:"database"`
	IndexName                string      `msgpack:"index_name"`
	IndexCreatedAt           int64       `msgpack:"index_created_at"`
	Metric                   string      `msgpack:"metric"`
	Dim                      uint32      `msgpack:"dim"`
	InternalDim              uint32      `msgpack:"internal_dim"`
	ProbeCentroidEpoch       uint64      `msgpack:"probe_centroid_epoch,omitempty"`
	ProbeCentroidBlob        []byte      `msgpack:"probe_centroid_blob,omitempty"`
	StableCentroidEpoch      uint64      `msgpack:"stable_centroid_epoch,omitempty"`
	StableCentroidBlob       []byte      `msgpack:"stable_centroid_blob,omitempty"`
	StableMemberCodecBlob    []byte      `msgpack:"stable_member_codec_blob,omitempty"`
	CentroidEpoch            uint64      `msgpack:"centroid_epoch,omitempty"`
	CentroidBlob             []byte      `msgpack:"centroid_blob,omitempty"`
	AppliedOverlaySeq        uint64      `msgpack:"applied_overlay_seq"`
	Generation               uint64      `msgpack:"generation"`
	DataFile                 string      `msgpack:"data_file"`
	DataFileSize             uint64      `msgpack:"data_file_size"`
	DataFileSHA256           string      `msgpack:"data_file_sha256"`
	RowMapFile               string      `msgpack:"rowmap_file"`
	RowMapFileSize           uint64      `msgpack:"rowmap_file_size"`
	RowMapFileSHA256         string      `msgpack:"rowmap_file_sha256"`
	BlockMetaFile            string      `msgpack:"block_meta_file,omitempty"`
	BlockMetaFileSize        uint64      `msgpack:"block_meta_file_size,omitempty"`
	BlockMetaFileSHA256      string      `msgpack:"block_meta_file_sha256,omitempty"`
	BlockRows                uint32      `msgpack:"block_rows,omitempty"`
	MaxCluster               uint32      `msgpack:"max_cluster"`
	RowCount                 uint64      `msgpack:"row_count"`
	ClusterRowCounts         []uint64    `msgpack:"cluster_row_counts,omitempty"`
	ClusterVectorSums        [][]float32 `msgpack:"cluster_vector_sums,omitempty"`
	RowsModifiedSinceRebuild uint64      `msgpack:"rows_modified_since_rebuild,omitempty"`
	LastRebuildRowCount      uint64      `msgpack:"last_rebuild_row_count,omitempty"`
	ConsecutiveSkewCycles    uint32      `msgpack:"consecutive_skew_cycles,omitempty"`
	LayoutHotClusters        []uint32    `msgpack:"layout_hot_clusters,omitempty"`
	CreatedAtUnixNano        int64       `msgpack:"created_at_unix_nano"`
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

func SegmentBlockPath(dir string, generation uint64) string {
	return SegmentBlockMetaPath(dir, generation)
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

func SegmentStoreV1Compat() uint32 {
	return segmentStoreV1
}

func SegmentStoreV2Compat() uint32 {
	return segmentStoreV2
}

func SegmentStoreV3Compat() uint32 {
	return segmentStoreV3
}

func (m *SegmentManifest) ProbeEpochValue() uint64 {
	if m == nil {
		return 0
	}
	if m.ProbeCentroidEpoch != 0 {
		return m.ProbeCentroidEpoch
	}
	return m.CentroidEpoch
}

func (m *SegmentManifest) StableEpochValue() uint64 {
	if m == nil {
		return 0
	}
	if m.StableCentroidEpoch != 0 {
		return m.StableCentroidEpoch
	}
	if m.CentroidEpoch != 0 {
		return m.CentroidEpoch
	}
	return m.ProbeCentroidEpoch
}

func (m *SegmentManifest) ProbeBlobValue() []byte {
	if m == nil {
		return nil
	}
	if len(m.ProbeCentroidBlob) > 0 {
		return m.ProbeCentroidBlob
	}
	return m.CentroidBlob
}

func (m *SegmentManifest) StableBlobValue() []byte {
	if m == nil {
		return nil
	}
	if len(m.StableCentroidBlob) > 0 {
		return m.StableCentroidBlob
	}
	if len(m.CentroidBlob) > 0 {
		return m.CentroidBlob
	}
	return m.ProbeCentroidBlob
}

func (m *SegmentManifest) NormalizeCentroidFields() {
	if m == nil {
		return
	}
	if m.ProbeCentroidEpoch == 0 {
		m.ProbeCentroidEpoch = m.CentroidEpoch
	}
	if len(m.ProbeCentroidBlob) == 0 {
		m.ProbeCentroidBlob = append([]byte(nil), m.CentroidBlob...)
	}
	if m.StableCentroidEpoch == 0 {
		if m.CentroidEpoch != 0 {
			m.StableCentroidEpoch = m.CentroidEpoch
		} else {
			m.StableCentroidEpoch = m.ProbeCentroidEpoch
		}
	}
	if len(m.StableCentroidBlob) == 0 {
		if len(m.CentroidBlob) > 0 {
			m.StableCentroidBlob = append([]byte(nil), m.CentroidBlob...)
		} else {
			m.StableCentroidBlob = append([]byte(nil), m.ProbeCentroidBlob...)
		}
	}
}
