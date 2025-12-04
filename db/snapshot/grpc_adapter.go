package snapshot

import (
	"io"
)

// GRPCChunk represents the snapshot chunk from gRPC (mirrors marmot.v2.SnapshotChunk)
type GRPCChunk interface {
	GetFilename() string
	GetChunkIndex() int32
	GetTotalChunks() int32
	GetData() []byte
	GetChecksum() string
	GetIsLastForFile() bool
}

// GRPCStream represents a gRPC streaming client that can receive chunks
type GRPCStream interface {
	Recv() (GRPCChunk, error)
}

// GRPCChunkAdapter wraps a gRPC stream to implement ChunkReceiver
type GRPCChunkAdapter struct {
	stream GRPCStream
}

// NewGRPCChunkAdapter creates a new adapter from a gRPC stream
func NewGRPCChunkAdapter(stream GRPCStream) *GRPCChunkAdapter {
	return &GRPCChunkAdapter{stream: stream}
}

// Recv implements ChunkReceiver by converting gRPC chunks to our Chunk type
func (a *GRPCChunkAdapter) Recv() (*Chunk, error) {
	grpcChunk, err := a.stream.Recv()
	if err != nil {
		return nil, err // Passes through io.EOF
	}

	return &Chunk{
		Filename:      grpcChunk.GetFilename(),
		ChunkIndex:    grpcChunk.GetChunkIndex(),
		TotalChunks:   grpcChunk.GetTotalChunks(),
		Data:          grpcChunk.GetData(),
		MD5Checksum:   grpcChunk.GetChecksum(),
		IsLastForFile: grpcChunk.GetIsLastForFile(),
	}, nil
}

// MarmotSnapshotStream wraps the concrete MarmotService_StreamSnapshotClient
// This is a type alias helper for easier usage in grpc and replica packages
type MarmotSnapshotStream[T GRPCChunk] interface {
	Recv() (T, error)
}

// GenericStreamAdapter wraps any gRPC stream that returns a concrete chunk type
type GenericStreamAdapter[T GRPCChunk] struct {
	stream MarmotSnapshotStream[T]
}

// NewGenericStreamAdapter creates an adapter for typed gRPC streams
func NewGenericStreamAdapter[T GRPCChunk](stream MarmotSnapshotStream[T]) *GenericStreamAdapter[T] {
	return &GenericStreamAdapter[T]{stream: stream}
}

// Recv implements ChunkReceiver
func (a *GenericStreamAdapter[T]) Recv() (*Chunk, error) {
	grpcChunk, err := a.stream.Recv()
	if err != nil {
		return nil, err
	}

	return &Chunk{
		Filename:      grpcChunk.GetFilename(),
		ChunkIndex:    grpcChunk.GetChunkIndex(),
		TotalChunks:   grpcChunk.GetTotalChunks(),
		Data:          grpcChunk.GetData(),
		MD5Checksum:   grpcChunk.GetChecksum(),
		IsLastForFile: grpcChunk.GetIsLastForFile(),
	}, nil
}

// ConvertDatabaseFileInfo converts gRPC DatabaseFileInfo to snapshot.DatabaseFileInfo
// This helper allows callers to convert without importing the snapshot package internals
func ConvertDatabaseFileInfo(
	name, filename string,
	sizeBytes int64,
	sha256Checksum string,
) DatabaseFileInfo {
	return DatabaseFileInfo{
		Name:           name,
		Filename:       filename,
		SizeBytes:      sizeBytes,
		SHA256Checksum: sha256Checksum,
	}
}

// Ensure adapters implement ChunkReceiver at compile time
var _ ChunkReceiver = (*GRPCChunkAdapter)(nil)
var _ ChunkReceiver = (*GenericStreamAdapter[GRPCChunk])(nil)

// io.EOF re-export for callers that need to check for end of stream
var EOF = io.EOF
