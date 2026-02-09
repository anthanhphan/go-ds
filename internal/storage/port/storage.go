package port

import (
	"context"
	"errors"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

var (
	ErrChunkNotFound = errors.New("chunk not found")
)

//ptr:
//go:generate mockgen -destination=../service/mocks/storage_mock.go -package=mocks -source=storage.go

// StorageRepository defines the interface for local storage operations.
type StorageRepository interface {
	// WriteChunk writes a chunk to the local storage using a reader for streaming.
	// It should be idempotent.
	WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error

	// ReadChunk reads a chunk from the local storage as a stream.
	ReadChunk(ctx context.Context, id domain.ChunkID) (checksum uint32, reader io.ReadCloser, err error)

	// HasChunk checks if a chunk exists locally (optimized for existence check).
	HasChunk(ctx context.Context, id domain.ChunkID) (bool, error)

	// DeleteChunk deletes a chunk from local storage.
	DeleteChunk(ctx context.Context, id domain.ChunkID) error

	// ListItemsInBucket returns all chunks mapping to a specific Merkle bucket.
	ListItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error)

	// ListAllKeys returns all chunk IDs currently in the index.
	ListAllKeys() []domain.ChunkID

	// GetParentID returns the parent file ID for a given chunk.
	GetParentID(id domain.ChunkID) (string, bool)

	// Merkle tree access for anti-entropy.
	GetMerkleRoot() string
	GetMerkleNodes(indices []int32) ([]shard.MerkleNode, error)

	// Compact reclaims disk space from deleted/stale data.
	Compact() error
}
