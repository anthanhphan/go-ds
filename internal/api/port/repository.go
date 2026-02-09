package port

import (
	"context"
	"io"

	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
)

//go:generate mockgen -destination=../service/mocks/storage_node_mock.go -package=mocks -source=repository.go

// StorageNode defines the interface for interacting with a storage node.
type StorageNode interface {
	// WriteChunk writes a chunk to a specific storage node via stream.
	WriteChunk(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error)

	// ReadChunk reads a chunk from a specific storage node via stream.
	ReadChunk(ctx context.Context, addr string, chunkID string, writer io.Writer) (*storagev1.ReadChunkResponse, error)

	// DeleteChunk deletes a chunk from a specific storage node.
	DeleteChunk(ctx context.Context, addr string, chunkID string) (*storagev1.DeleteChunkResponse, error)
}
