package service

import (
	"context"
	"fmt"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

// chunkOpsService handles direct local chunk write/read/delete operations.
type chunkOpsService struct {
	core *StorageServiceImpl
}

// newChunkOpsService creates the chunk operations use-case service.
func newChunkOpsService(core *StorageServiceImpl) *chunkOpsService {
	return &chunkOpsService{core: core}
}

// writeChunk validates ownership and writes chunk bytes to local repository.
func (s *chunkOpsService) writeChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
	if !s.ownsChunk(id) {
		err := fmt.Errorf("node %s is not a replica owner for chunk %s", s.core.nodeID, id)
		logger.Warnw("WriteChunk rejected by ownership check", "chunk_id", id, "node_id", s.core.nodeID, "error", err.Error())
		return err
	}

	if err := s.core.storage.WriteChunk(ctx, id, parentID, checksum, reader); err != nil {
		logger.Errorw("WriteChunk failed", "chunk_id", id, "parent_id", parentID, "error", err.Error())
		return err
	}
	return nil
}

// readChunk reads chunk payload and checksum from local repository.
func (s *chunkOpsService) readChunk(ctx context.Context, id domain.ChunkID) (uint32, io.ReadCloser, error) {
	return s.core.storage.ReadChunk(ctx, id)
}

// deleteChunk deletes a local chunk and logs failures for observability.
func (s *chunkOpsService) deleteChunk(ctx context.Context, id domain.ChunkID) error {
	if err := s.core.storage.DeleteChunk(ctx, id); err != nil {
		logger.Warnw("DeleteChunk failed", "chunk_id", id, "error", err.Error())
		return err
	}
	return nil
}

// ownsChunk checks whether this node is a runtime replica owner for chunk ID.
func (s *chunkOpsService) ownsChunk(id domain.ChunkID) bool {
	token := murmur3.Sum64([]byte(id))
	strategy := shard.NewShardStrategy(s.core.ring)
	replicas := strategy.GetReplicas(token, 0)
	if len(replicas) == 0 {
		// Fail-open avoids dropping writes during transient topology warmup.
		logger.Warnw("Ownership check fallback allow write due empty replica set", "chunk_id", id, "node_id", s.core.nodeID)
		return true
	}

	return s.isReplicaOwner(replicas)
}

// isReplicaOwner checks if current node ID appears in replica set.
func (s *chunkOpsService) isReplicaOwner(replicas []shard.Node) bool {
	for _, replica := range replicas {
		if replica.ID == s.core.nodeID {
			return true
		}
	}
	return false
}
