package grpc

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/maxpert/marmot/cfg"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/encoding"
)

const zstdName = "zstd"

// zstdCompressor implements gRPC's encoding.Compressor interface using zstd
type zstdCompressor struct {
	level       zstd.EncoderLevel
	encoderPool sync.Pool
	decoderPool sync.Pool
}

// init registers the zstd compressor with gRPC
func init() {
	RegisterZstdCompressor()
}

// RegisterZstdCompressor registers the zstd compressor with gRPC encoding
func RegisterZstdCompressor() {
	level := getCompressionLevel()
	if level == 0 {
		log.Debug().Msg("gRPC compression disabled (level=0)")
		return
	}

	zstdLevel := configLevelToZstd(level)
	c := &zstdCompressor{
		level: zstdLevel,
	}

	encoding.RegisterCompressor(c)
	log.Info().
		Int("config_level", level).
		Str("zstd_level", zstdLevel.String()).
		Msg("Registered zstd gRPC compressor")
}

// Name returns the compressor name
func (c *zstdCompressor) Name() string {
	return zstdName
}

// Compress returns a WriteCloser that compresses data written to it
func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	// Try to get encoder from pool
	if enc, ok := c.encoderPool.Get().(*zstd.Encoder); ok {
		enc.Reset(w)
		return &pooledEncoder{enc: enc, pool: &c.encoderPool}, nil
	}

	// Create new encoder
	enc, err := zstd.NewWriter(w, zstd.WithEncoderLevel(c.level))
	if err != nil {
		return nil, err
	}
	return &pooledEncoder{enc: enc, pool: &c.encoderPool}, nil
}

// Decompress returns a Reader that decompresses data read from it
func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	// Try to get decoder from pool
	if dec, ok := c.decoderPool.Get().(*zstd.Decoder); ok {
		if err := dec.Reset(r); err != nil {
			c.decoderPool.Put(dec)
			return nil, err
		}
		return &pooledDecoder{dec: dec, pool: &c.decoderPool}, nil
	}

	// Create new decoder
	dec, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &pooledDecoder{dec: dec, pool: &c.decoderPool}, nil
}

// pooledEncoder wraps zstd.Encoder to return it to pool on Close
type pooledEncoder struct {
	enc  *zstd.Encoder
	pool *sync.Pool
}

func (p *pooledEncoder) Write(data []byte) (int, error) {
	return p.enc.Write(data)
}

func (p *pooledEncoder) Close() error {
	err := p.enc.Close()
	p.pool.Put(p.enc)
	return err
}

// pooledDecoder wraps zstd.Decoder to return it to pool when done
type pooledDecoder struct {
	dec  *zstd.Decoder
	pool *sync.Pool
}

func (p *pooledDecoder) Read(data []byte) (int, error) {
	n, err := p.dec.Read(data)
	if err == io.EOF {
		// Return decoder to pool on EOF
		p.pool.Put(p.dec)
	}
	return n, err
}

// getCompressionLevel returns the configured compression level
func getCompressionLevel() int {
	if cfg.Config == nil {
		return 1 // Default to fastest
	}
	return cfg.Config.GRPCClient.CompressionLevel
}

// configLevelToZstd maps config levels (1-4) to zstd.EncoderLevel
func configLevelToZstd(level int) zstd.EncoderLevel {
	switch level {
	case 1:
		return zstd.SpeedFastest // ~318 MB/s, ratio ~2.88x
	case 2:
		return zstd.SpeedDefault // ~134 MB/s, ratio ~3.0x
	case 3:
		return zstd.SpeedBetterCompression // ~67 MB/s, ratio ~3.2x
	case 4:
		return zstd.SpeedBestCompression // ~12 MB/s, ratio ~3.5x
	default:
		return zstd.SpeedFastest
	}
}

// IsCompressionEnabled returns true if gRPC compression is enabled
func IsCompressionEnabled() bool {
	return getCompressionLevel() > 0
}

// GetCompressionName returns the compression codec name if enabled, empty otherwise
func GetCompressionName() string {
	if IsCompressionEnabled() {
		return zstdName
	}
	return ""
}
