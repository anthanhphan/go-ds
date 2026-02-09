package service

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/spaolacci/murmur3"
)

// chunkWriteService is responsible for chunk replication with write consistency.
type chunkWriteService struct {
	core *FileServiceImpl
}

// newChunkWriteService creates a chunk write use-case service.
func newChunkWriteService(core *FileServiceImpl) *chunkWriteService {
	return &chunkWriteService{core: core}
}

// ReplicateChunk writes one chunk to replica nodes and enforces write consistency.
func (s *chunkWriteService) ReplicateChunk(ctx context.Context, fileID string, chunkIndex int, chunkData []byte) error {
	consistency := s.core.getWriteConsistency()
	chunkID := buildChunkID(fileID, chunkIndex)
	checksum := crc32.ChecksumIEEE(chunkData)

	replicas, requiredSuccess, err := s.resolveChunkReplicas(chunkID, consistency)
	if err != nil {
		return fmt.Errorf("chunk %d cannot satisfy consistency: %w", chunkIndex, err)
	}

	successCount, writeErr := s.core.executeReplicaWrites(
		ctx,
		replicas,
		requiredSuccess,
		func(execCtx context.Context, node shard.Node) error {
			resp, err := s.core.storage.WriteChunk(execCtx, node.Addr, string(chunkID), fileID, checksum, bytes.NewReader(chunkData))
			if err != nil {
				return err
			}
			if resp == nil || !resp.Success {
				msg := "empty response"
				if resp != nil && resp.Message != "" {
					msg = resp.Message
				}
				return fmt.Errorf("refused: %s", msg)
			}
			return nil
		},
		func(node shard.Node, err error) error {
			return fmt.Errorf("node %s failed: %w", node.ID, err)
		},
	)
	if writeErr != nil {
		return fmt.Errorf(
			"chunk %d consistency failed (success=%d required=%d total=%d): %w",
			chunkIndex,
			successCount,
			requiredSuccess,
			len(replicas),
			writeErr,
		)
	}

	return nil
}

// resolveChunkReplicas finds runtime replicas and required ACK threshold for a chunk.
func (s *chunkWriteService) resolveChunkReplicas(chunkID domain.ChunkID, consistency port.ConsistencyLevel) ([]shard.Node, int, error) {
	token := murmur3.Sum64([]byte(chunkID))
	strategy := shard.NewShardStrategy(s.core.ring)
	replicas := strategy.GetReplicas(token, 0)
	if len(replicas) == 0 {
		return nil, 0, fmt.Errorf("no replica available for chunk")
	}

	requiredSuccess, err := s.core.requiredSuccess(consistency, len(replicas))
	if err != nil {
		return nil, 0, err
	}
	return replicas, requiredSuccess, nil
}
