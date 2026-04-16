package vecindex

import (
	"fmt"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
)

var (
	blobEncoderOnce sync.Once
	blobDecoderOnce sync.Once
	blobEncoder     *zstd.Encoder
	blobDecoder     *zstd.Decoder
)

func getEncoder() *zstd.Encoder {
	blobEncoderOnce.Do(func() {
		var err error
		blobEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		if err != nil {
			panic(fmt.Sprintf("vecindex: init zstd encoder: %v", err))
		}
	})
	return blobEncoder
}

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

// EncodeCentroidBlob serialises a CentroidSet to a zstd-compressed msgpack blob.
// This is the format stored in _marmot_vec_<idx>_centroids.centroids per design §3.2.
func EncodeCentroidBlob(cs *kmeans.CentroidSet) ([]byte, error) {
	raw, err := cs.Encode()
	if err != nil {
		return nil, fmt.Errorf("centroid blob: encode msgpack: %w", err)
	}
	return getEncoder().EncodeAll(raw, nil), nil
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
