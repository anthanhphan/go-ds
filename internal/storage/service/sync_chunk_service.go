package service

import (
	"context"
	"fmt"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/gosdk/logger"
)

// syncChunkService fetches and stores chunks for repair/handoff flows.
type syncChunkService struct {
	core *StorageServiceImpl
}

// newSyncChunkService creates sync use-case service.
func newSyncChunkService(core *StorageServiceImpl) *syncChunkService {
	return &syncChunkService{core: core}
}

// syncChunk ensures chunk exists locally by pulling from a peer when needed.
func (s *syncChunkService) syncChunk(ctx context.Context, chunkID string, sourceAddr string) error {
	exists, err := s.core.storage.HasChunk(ctx, domain.ChunkID(chunkID))
	if err == nil && exists {
		logger.Debugw("SyncChunk skipped, already present", "chunk_id", chunkID)
		return nil
	}

	logger.Infow("SyncChunk fetching", "chunk_id", chunkID, "source", sourceAddr)
	return s.fetchAndStoreChunk(ctx, chunkID, sourceAddr)
}

// fetchAndStoreChunk streams chunk bytes from peer and writes to local storage.
func (s *syncChunkService) fetchAndStoreChunk(ctx context.Context, chunkID string, sourceAddr string) error {
	checksum, reader, err := s.core.replication.FetchChunk(ctx, sourceAddr, chunkID)
	if err != nil {
		return fmt.Errorf("failed to fetch chunk from peer: %w", err)
	}
	defer func() { _ = reader.Close() }()

	if err := s.core.storage.WriteChunk(ctx, domain.ChunkID(chunkID), "", checksum, reader); err != nil {
		return fmt.Errorf("failed to store fetched chunk: %w", err)
	}

	logger.Debugw("SyncChunk stored", "chunk_id", chunkID, "source", sourceAddr)
	return nil
}
