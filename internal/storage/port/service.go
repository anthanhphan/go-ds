package port

import (
	"context"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

// StorageService defines the business logic for storage operations.
type StorageService interface {
	// WriteChunk validates and writes a chunk from a stream.
	WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error

	// ReadChunk reads a chunk as a stream.
	ReadChunk(ctx context.Context, id domain.ChunkID) (checksum uint32, reader io.ReadCloser, err error)

	// DeleteChunk deletes a chunk.
	DeleteChunk(ctx context.Context, id domain.ChunkID) error

	// GetClusterTopology returns the current cluster topology (list of nodes).
	GetClusterTopology(ctx context.Context) ([]shard.Node, error)

	// Anti-Entropy methods
	GetMerkleRoot(ctx context.Context) (string, error)
	GetMerkleNodes(ctx context.Context, indices []int32) ([]shard.MerkleNode, error)
	ListItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error)
	SyncChunk(ctx context.Context, chunkID string, sourceAddr string) error
	GarbageCollect(ctx context.Context, gracePeriodSeconds int32) (int32, int64, error)
}
