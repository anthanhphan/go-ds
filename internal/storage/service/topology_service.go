package service

import (
	"context"

	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

// topologyQueryService provides ring and Merkle read-only queries.
type topologyQueryService struct {
	core *StorageServiceImpl
}

// newTopologyQueryService creates topology query use-case service.
func newTopologyQueryService(core *StorageServiceImpl) *topologyQueryService {
	return &topologyQueryService{core: core}
}

// getClusterTopology returns all current storage nodes from the ring.
func (s *topologyQueryService) getClusterTopology(ctx context.Context) ([]shard.Node, error) {
	return s.core.ring.GetNodes(), nil
}

// getMerkleRoot returns local Merkle root from storage repository.
func (s *topologyQueryService) getMerkleRoot(ctx context.Context) (string, error) {
	return s.core.storage.GetMerkleRoot(), nil
}

// getMerkleNodes returns selected local Merkle nodes.
func (s *topologyQueryService) getMerkleNodes(ctx context.Context, indices []int32) ([]shard.MerkleNode, error) {
	return s.core.storage.GetMerkleNodes(indices)
}

// listItemsInBucket returns chunk summaries for one bucket or all buckets.
func (s *topologyQueryService) listItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error) {
	return s.core.storage.ListItemsInBucket(ctx, bucketID)
}
