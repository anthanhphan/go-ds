package port

import (
	"context"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

//go:generate mockgen -destination=../service/mocks/replication_mock.go -package=mocks -source=replication.go

// ReplicationPort defines the interface for replicating data to peer nodes.
type ReplicationPort interface {
	// ReplicateChunk sends a chunk to a specific target node.
	ReplicateChunk(ctx context.Context, target shard.Node, chunk domain.Chunk) error

	// Peer-to-peer Anti-Entropy methods
	GetMerkleRoot(ctx context.Context, targetAddr string) (string, error)
	GetMerkleNodes(ctx context.Context, targetAddr string, indices []int32) ([]shard.MerkleNode, error)
	ListItemsInBucket(ctx context.Context, targetAddr string, bucketID int32) ([]shard.BucketItem, error)
	FetchChunk(ctx context.Context, targetAddr string, chunkID string) (uint32, io.ReadCloser, error)
}
