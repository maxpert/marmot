package vecindex

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

var (
	blobDecoderOnce sync.Once
	blobDecoder     *zstd.Decoder
)

func getDecoder() *zstd.Decoder {
	blobDecoderOnce.Do(func() {
		var err error
		blobDecoder, err = zstd.NewReader(nil)
		if err != nil {
			panic(fmt.Sprintf("vecindex: init zstd decoder: %v", err))
		}
	})
	return blobDecoder
}

func encodeMetadataBlob(raw []byte) ([]byte, error) {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedFastest),
		zstd.WithEncoderConcurrency(1),
		zstd.WithLowerEncoderMem(true),
		zstd.WithWindowSize(1<<20),
	)
	if err != nil {
		return nil, err
	}
	defer enc.Close()
	return enc.EncodeAll(raw, nil), nil
}

// EncodeCentroidBlob serialises a CentroidSet to a zstd-compressed msgpack
// blob. The active centroid blob is embedded in the local segment manifest.
func EncodeCentroidBlob(cs *kmeans.CentroidSet) ([]byte, error) {
	raw, err := cs.Encode()
	if err != nil {
		return nil, fmt.Errorf("centroid blob: encode msgpack: %w", err)
	}
	blob, err := encodeMetadataBlob(raw)
	if err != nil {
		return nil, fmt.Errorf("centroid blob: zstd compress: %w", err)
	}
	return blob, nil
}

// DecodeCentroidBlob deserialises a CentroidSet from a zstd-compressed msgpack blob.
func DecodeCentroidBlob(data []byte) (*kmeans.CentroidSet, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("centroid blob: empty data")
	}
	raw, err := getDecoder().DecodeAll(data, nil)
	if err != nil {
		return nil, fmt.Errorf("centroid blob: zstd decompress: %w", err)
	}
	cs, err := kmeans.DecodeCentroidSet(raw)
	if err != nil {
		return nil, fmt.Errorf("centroid blob: decode: %w", err)
	}
	return cs, nil
}
