package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/spaolacci/murmur3"
)

// metadataService handles metadata write/read workflows with consistency.
type metadataService struct {
	core *FileServiceImpl
}

// newMetadataService creates the metadata use-case service.
func newMetadataService(core *FileServiceImpl) *metadataService {
	return &metadataService{core: core}
}

// SaveMetadata persists file metadata to replicas using write consistency.
func (s *metadataService) SaveMetadata(ctx context.Context, metaID string, data []byte) error {
	consistency := s.core.getWriteConsistency()
	replicas, requiredSuccess, err := s.resolveMetadataReplicas(metaID, consistency)
	if err != nil {
		return fmt.Errorf("metadata write cannot satisfy consistency for %s: %w", metaID, err)
	}

	checksum := crc32.ChecksumIEEE(data)
	successCount, writeErr := s.core.executeReplicaWrites(
		ctx,
		replicas,
		requiredSuccess,
		func(execCtx context.Context, node shard.Node) error {
			resp, err := s.core.storage.WriteChunk(execCtx, node.Addr, metaID, "meta", checksum, bytes.NewReader(data))
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
			return fmt.Errorf("meta write failed on node %s: %w", node.ID, err)
		},
	)
	if writeErr != nil {
		return fmt.Errorf(
			"metadata consistency failed for %s (success=%d required=%d total=%d): %w",
			metaID,
			successCount,
			requiredSuccess,
			len(replicas),
			writeErr,
		)
	}

	return nil
}

// getFileMetadata reads and decodes metadata with configured read consistency.
func (s *metadataService) getFileMetadata(ctx context.Context, fileID string) (*domain.FileMetadata, error) {
	consistency := s.core.getReadConsistency()
	metaID := buildMetadataID(fileID)

	replicas, requiredSuccess, err := s.resolveMetadataReplicas(metaID, consistency)
	if err != nil {
		return nil, fmt.Errorf("metadata read cannot satisfy consistency for %s: %w", fileID, err)
	}

	type readResult struct {
		meta *domain.FileMetadata
		err  error
	}
	resultChan := make(chan readResult, len(replicas))

	for _, replica := range replicas {
		go func(node shard.Node) {
			meta, readErr := s.readMetadataFromReplica(ctx, node, metaID)
			select {
			case resultChan <- readResult{meta: meta, err: readErr}:
			case <-ctx.Done():
			}
		}(replica)
	}

	successCount := 0
	failCount := 0
	var foundMeta *domain.FileMetadata
	var lastErr error

	for i := 0; i < len(replicas); i++ {
		select {
		case res := <-resultChan:
			if res.err == nil {
				successCount++
				if foundMeta == nil {
					foundMeta = res.meta
				}
				if successCount >= requiredSuccess {
					return foundMeta, nil
				}
				continue
			}

			failCount++
			lastErr = res.err
			if failCount > len(replicas)-requiredSuccess {
				return nil, fmt.Errorf(
					"metadata consistency failed for %s (success=%d required=%d total=%d): %w",
					fileID,
					successCount,
					requiredSuccess,
					len(replicas),
					lastErr,
				)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if foundMeta == nil {
		return nil, fmt.Errorf("metadata not found for %s", fileID)
	}
	return foundMeta, nil
}

// resolveMetadataReplicas finds metadata replicas and required ACK threshold.
func (s *metadataService) resolveMetadataReplicas(metaID string, consistency port.ConsistencyLevel) ([]shard.Node, int, error) {
	token := murmur3.Sum64([]byte(metaID))
	strategy := shard.NewShardStrategy(s.core.ring)
	replicas := strategy.GetReplicas(token, 0)
	if len(replicas) == 0 {
		return nil, 0, fmt.Errorf("no replica available for metadata %s", metaID)
	}

	requiredSuccess, err := s.core.requiredSuccess(consistency, len(replicas))
	if err != nil {
		return nil, 0, err
	}
	return replicas, requiredSuccess, nil
}

// readMetadataFromReplica reads metadata bytes from one replica and decodes JSON.
func (s *metadataService) readMetadataFromReplica(ctx context.Context, node shard.Node, metaID string) (*domain.FileMetadata, error) {
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	var buf bytes.Buffer
	resp, err := s.core.storage.ReadChunk(readCtx, node.Addr, metaID, &buf)
	if err != nil {
		return nil, err
	}
	if resp == nil || !resp.Found {
		return nil, port.ErrChunkNotFound
	}

	var meta domain.FileMetadata
	if err := json.Unmarshal(buf.Bytes(), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}
