package service

import (
	"context"
	"io"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

// StorageServiceImpl is a facade that composes storage use-case services.
type StorageServiceImpl struct {
	storage     port.StorageRepository
	replication port.ReplicationPort
	ring        *shard.Ring
	nodeID      string
	shardID     string

	chunkOps    *chunkOpsService
	topology    *topologyQueryService
	syncer      *syncChunkService
	gc          *garbageCollectorService
	antiEntropy *antiEntropyService
}

// Ensure StorageServiceImpl implements port.StorageService.
var _ port.StorageService = (*StorageServiceImpl)(nil)

// NewStorageService builds storage facade and all use-case services.
func NewStorageService(storage port.StorageRepository, replication port.ReplicationPort, ring *shard.Ring, nodeID, shardID string) *StorageServiceImpl {
	svc := &StorageServiceImpl{
		storage:     storage,
		replication: replication,
		ring:        ring,
		nodeID:      nodeID,
		shardID:     shardID,
	}

	svc.chunkOps = newChunkOpsService(svc)
	svc.topology = newTopologyQueryService(svc)
	svc.syncer = newSyncChunkService(svc)
	svc.gc = newGarbageCollectorService(svc)
	svc.antiEntropy = newAntiEntropyService(svc)

	return svc
}

// WriteChunk validates ownership and stores an incoming chunk.
func (s *StorageServiceImpl) WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
	return s.chunkOps.writeChunk(ctx, id, parentID, checksum, reader)
}

// ReadChunk returns a chunk stream and checksum from local storage.
func (s *StorageServiceImpl) ReadChunk(ctx context.Context, id domain.ChunkID) (uint32, io.ReadCloser, error) {
	return s.chunkOps.readChunk(ctx, id)
}

// DeleteChunk removes a chunk from local storage.
func (s *StorageServiceImpl) DeleteChunk(ctx context.Context, id domain.ChunkID) error {
	return s.chunkOps.deleteChunk(ctx, id)
}

// GetClusterTopology returns the current storage node view from the ring.
func (s *StorageServiceImpl) GetClusterTopology(ctx context.Context) ([]shard.Node, error) {
	return s.topology.getClusterTopology(ctx)
}

// GetMerkleRoot returns current local Merkle root for anti-entropy.
func (s *StorageServiceImpl) GetMerkleRoot(ctx context.Context) (string, error) {
	return s.topology.getMerkleRoot(ctx)
}

// GetMerkleNodes returns selected Merkle nodes by indices.
func (s *StorageServiceImpl) GetMerkleNodes(ctx context.Context, indices []int32) ([]shard.MerkleNode, error) {
	return s.topology.getMerkleNodes(ctx, indices)
}

// ListItemsInBucket lists chunk fingerprints in a Merkle bucket.
func (s *StorageServiceImpl) ListItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error) {
	return s.topology.listItemsInBucket(ctx, bucketID)
}

// SyncChunk fetches a chunk from peer and stores it locally.
func (s *StorageServiceImpl) SyncChunk(ctx context.Context, chunkID string, sourceAddr string) error {
	return s.syncer.syncChunk(ctx, chunkID, sourceAddr)
}

// GarbageCollect removes orphaned chunks and triggers compaction.
func (s *StorageServiceImpl) GarbageCollect(ctx context.Context, gracePeriodSeconds int32) (int32, int64, error) {
	return s.gc.garbageCollect(ctx, gracePeriodSeconds)
}

// StartAntiEntropyWorker runs periodic reconciliation against peers.
func (s *StorageServiceImpl) StartAntiEntropyWorker(ctx context.Context, interval time.Duration) {
	s.antiEntropy.startWorker(ctx, interval)
}
