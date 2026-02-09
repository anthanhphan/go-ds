package service

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

// downloadService reconstructs files from replicated chunks.
type downloadService struct {
	core     *FileServiceImpl
	metadata *metadataService
}

// newDownloadService creates the download use-case service.
func newDownloadService(core *FileServiceImpl, metadata *metadataService) *downloadService {
	return &downloadService{core: core, metadata: metadata}
}

// downloadFile fetches metadata and streams all chunks to the output writer.
func (s *downloadService) downloadFile(ctx context.Context, fileID string, writer io.Writer) error {
	logger.Infow("Download started", "file_id", fileID)

	meta, err := s.metadata.getFileMetadata(ctx, fileID)
	if err != nil {
		return err
	}

	readConsistency := s.core.getReadConsistency()
	for chunkIndex := 0; chunkIndex < meta.Chunks; chunkIndex++ {
		chunkData, err := s.readChunkWithConsistency(ctx, fileID, chunkIndex, readConsistency)
		if err != nil {
			return err
		}
		if _, err := writer.Write(chunkData); err != nil {
			return fmt.Errorf("failed to write to output: %w", err)
		}
	}

	logger.Infow("Download completed", "file_id", fileID, "chunks", meta.Chunks)
	return nil
}

// readChunkWithConsistency reads one chunk from replicas and enforces read consistency.
func (s *downloadService) readChunkWithConsistency(
	ctx context.Context,
	fileID string,
	chunkIndex int,
	consistency port.ConsistencyLevel,
) ([]byte, error) {
	chunkID := buildChunkID(fileID, chunkIndex)
	replicas, requiredSuccess, err := s.resolveChunkReadReplicas(chunkID, consistency)
	if err != nil {
		return nil, fmt.Errorf("chunk %d cannot satisfy consistency: %w", chunkIndex, err)
	}

	type readResult struct {
		data []byte
		err  error
	}
	resultChan := make(chan readResult, len(replicas))

	for _, replica := range replicas {
		go func(node shard.Node) {
			data, readErr := s.readChunkFromReplica(ctx, node, chunkID)
			select {
			case resultChan <- readResult{data: data, err: readErr}:
			case <-ctx.Done():
			}
		}(replica)
	}

	successCount := 0
	failCount := 0
	var selectedChunk []byte
	var lastErr error

	for i := 0; i < len(replicas); i++ {
		select {
		case res := <-resultChan:
			if res.err == nil {
				successCount++
				if selectedChunk == nil {
					selectedChunk = res.data
				}
				if successCount >= requiredSuccess {
					return selectedChunk, nil
				}
				continue
			}

			failCount++
			lastErr = res.err
			if failCount > len(replicas)-requiredSuccess {
				return nil, fmt.Errorf(
					"chunk %d consistency failed (success: %d/%d): %w",
					chunkIndex,
					successCount,
					len(replicas),
					lastErr,
				)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	if selectedChunk == nil {
		return nil, fmt.Errorf("chunk %d data empty", chunkIndex)
	}
	return selectedChunk, nil
}

// readChunkFromReplica reads and verifies checksum for a chunk from one replica.
func (s *downloadService) readChunkFromReplica(ctx context.Context, node shard.Node, chunkID domain.ChunkID) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var chunkBuf bytes.Buffer
	resp, err := s.core.storage.ReadChunk(readCtx, node.Addr, string(chunkID), &chunkBuf)
	if err != nil {
		return nil, err
	}
	if resp == nil || !resp.Found {
		return nil, port.ErrChunkNotFound
	}

	actualChecksum := crc32.ChecksumIEEE(chunkBuf.Bytes())
	if actualChecksum != resp.Checksum {
		return nil, fmt.Errorf("checksum mismatch")
	}

	return chunkBuf.Bytes(), nil
}

// resolveChunkReadReplicas finds chunk replicas and required read ACK threshold.
func (s *downloadService) resolveChunkReadReplicas(chunkID domain.ChunkID, consistency port.ConsistencyLevel) ([]shard.Node, int, error) {
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
