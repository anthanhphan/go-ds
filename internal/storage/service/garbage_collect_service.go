package service

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

// garbageCollectorService removes orphaned chunks after metadata checks.
type garbageCollectorService struct {
	core *StorageServiceImpl
}

// newGarbageCollectorService creates garbage collection use-case service.
func newGarbageCollectorService(core *StorageServiceImpl) *garbageCollectorService {
	return &garbageCollectorService{core: core}
}

// garbageCollect scans local chunks, deletes orphans, and triggers compaction.
func (s *garbageCollectorService) garbageCollect(ctx context.Context, gracePeriodSeconds int32) (int32, int64, error) {
	keys := s.core.storage.ListAllKeys()
	logger.Infow("Garbage collection started", "local_chunks", len(keys), "grace_period_seconds", gracePeriodSeconds)

	var deletedCount int32
	var bytesReclaimed int64

	for _, id := range keys {
		deleted, reclaimed := s.evaluateChunkForGC(ctx, id)
		if deleted {
			deletedCount++
			bytesReclaimed += reclaimed
		}
	}

	s.maybeTriggerCompaction(deletedCount)
	logger.Infow("Garbage collection finished", "deleted_chunks", deletedCount, "bytes_reclaimed", bytesReclaimed)
	return deletedCount, bytesReclaimed, nil
}

// evaluateChunkForGC evaluates one chunk and deletes it if confirmed orphaned.
func (s *garbageCollectorService) evaluateChunkForGC(ctx context.Context, id domain.ChunkID) (bool, int64) {
	if strings.HasPrefix(string(id), "meta:") {
		return false, 0
	}

	parentID, ok := s.core.storage.GetParentID(id)
	if !ok || parentID == "" {
		return false, 0
	}

	metaID := "meta:" + parentID
	replicas := s.metadataReplicas(metaID)
	if len(replicas) == 0 {
		logger.Warnw("Skipping GC check, no replicas found for metadata", "chunk_id", id, "meta_id", metaID)
		return false, 0
	}

	exists, checkErr := s.metadataExists(ctx, metaID, replicas)
	if checkErr != nil {
		logger.Warnw("Skipping GC delete due metadata lookup error", "chunk_id", id, "meta_id", metaID, "error", checkErr.Error())
		return false, 0
	}
	if exists {
		return false, 0
	}

	reclaimed := s.estimateChunkSize(ctx, id)
	logger.Infow("GC deleting orphaned chunk", "chunk_id", id, "parent_id", parentID, "meta_id", metaID)
	if err := s.core.storage.DeleteChunk(ctx, id); err != nil {
		logger.Warnw("GC delete failed", "chunk_id", id, "error", err.Error())
		return false, 0
	}

	return true, reclaimed
}

// metadataReplicas returns runtime replica set responsible for metadata key.
func (s *garbageCollectorService) metadataReplicas(metaID string) []shard.Node {
	token := murmur3.Sum64([]byte(metaID))
	strategy := shard.NewShardStrategy(s.core.ring)
	return strategy.GetReplicas(token, 0)
}

// metadataExists checks metadata presence locally first, then on peer replicas.
func (s *garbageCollectorService) metadataExists(ctx context.Context, metaID string, replicas []shard.Node) (bool, error) {
	if exists, err := s.core.storage.HasChunk(ctx, domain.ChunkID(metaID)); err == nil && exists {
		return true, nil
	}

	var firstErr error
	for _, node := range replicas {
		if node.ID == s.core.nodeID {
			continue
		}

		readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, reader, err := s.core.replication.FetchChunk(readCtx, node.Addr, metaID)
		cancel()
		if err == nil {
			defer func() { _ = reader.Close() }()
			_, _ = io.Copy(io.Discard, reader)
			return true, nil
		}
		if errors.Is(err, port.ErrChunkNotFound) {
			continue
		}
		if firstErr == nil {
			firstErr = err
		}
	}

	return false, firstErr
}

// estimateChunkSize best-effort reads chunk to count reclaimable bytes.
func (s *garbageCollectorService) estimateChunkSize(ctx context.Context, id domain.ChunkID) int64 {
	_, reader, err := s.core.storage.ReadChunk(ctx, id)
	if err != nil {
		return 0
	}
	defer func() { _ = reader.Close() }()

	size, copyErr := io.Copy(io.Discard, reader)
	if copyErr != nil {
		return 0
	}
	return size
}

// maybeTriggerCompaction starts async compaction when at least one chunk was deleted.
func (s *garbageCollectorService) maybeTriggerCompaction(deletedCount int32) {
	if deletedCount <= 0 {
		return
	}

	go func() {
		if err := s.core.storage.Compact(); err != nil {
			logger.Warnw("GC compaction failed", "error", err.Error())
		}
	}()
}
