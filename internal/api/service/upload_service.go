package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/resilience"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

//go:generate mockgen -destination=mocks/dependencies_mock.go -package=mocks -source=upload_service.go

// ChunkReplicator defines chunk replication capability.
type ChunkReplicator interface {
	ReplicateChunk(ctx context.Context, fileID string, chunkIndex int, chunkData []byte) error
}

// MetadataPersistence defines metadata saving capability.
type MetadataPersistence interface {
	SaveMetadata(ctx context.Context, metaID string, data []byte) error
}

// IDGenerator defines ID generation capability.
type IDGenerator interface {
	Next() (int64, error)
}

// uploadService orchestrates chunking, replication, and metadata persistence.
type uploadService struct {
	core     *FileServiceImpl
	chunker  ChunkReplicator
	metadata MetadataPersistence
	idGen    IDGenerator
}

// uploadStats tracks aggregate stats while processing file chunks.
type uploadStats struct {
	totalSize  int64
	chunkCount int
}

// newUploadService creates the upload use-case service.
func newUploadService(core *FileServiceImpl, chunker ChunkReplicator, metadata MetadataPersistence, idGen IDGenerator) *uploadService {
	return &uploadService{core: core, chunker: chunker, metadata: metadata, idGen: idGen}
}

// uploadFile performs the full upload workflow from stream to metadata.
func (s *uploadService) uploadFile(ctx context.Context, fileName string, reader io.Reader) (string, error) {
	fileID, err := s.nextFileID()
	if err != nil {
		return "", err
	}

	logger.Infow("Upload started", "file_id", fileID, "file_name", fileName)

	stats, err := s.streamAndReplicateChunks(ctx, fileID, reader)
	if err != nil {
		logger.Errorw("Upload failed", "file_id", fileID, "error", err.Error())
		s.cleanupUpload(fileID, stats.chunkCount)
		return "", err
	}

	if err := s.persistFileMetadata(ctx, fileID, fileName, stats); err != nil {
		logger.Errorw("Metadata persistence failed", "file_id", fileID, "error", err.Error())
		s.cleanupUpload(fileID, stats.chunkCount)
		return "", err
	}

	logger.Infow("Upload completed", "file_id", fileID, "chunks", stats.chunkCount, "size_bytes", stats.totalSize)
	return fileID, nil
}

// nextFileID allocates a distributed unique file ID from the configured generator.
func (s *uploadService) nextFileID() (string, error) {
	id, err := s.idGen.Next()
	if err != nil {
		return "", fmt.Errorf("failed to generate snowflake id: %w", err)
	}
	return fmt.Sprintf("%d", id), nil
}

// streamAndReplicateChunks reads file data chunk-by-chunk and replicates in parallel.
func (s *uploadService) streamAndReplicateChunks(ctx context.Context, fileID string, reader io.Reader) (uploadStats, error) {
	numWorkers := s.core.cfg.App.ParallelChunks
	if numWorkers <= 0 {
		numWorkers = 4
	}

	workerPool := resilience.NewWorkerPool(numWorkers, numWorkers*2)
	var stats uploadStats
	var statsMu sync.Mutex
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var uploadErr error
	var errOnce sync.Once
	reportErr := func(err error) {
		if err == nil {
			return
		}
		errOnce.Do(func() {
			uploadErr = err
			cancelWorkers()
		})
	}

	chunkIndex := 0
produceLoop:
	for {
		select {
		case <-workerCtx.Done():
			break produceLoop
		default:
		}

		buffer := s.core.pool.Get().(*[]byte)
		readN, readErr := io.ReadFull(reader, *buffer)
		if readN > 0 {
			if err := s.submitChunkReplication(workerCtx, workerPool, fileID, chunkIndex, buffer, readN, &stats, &statsMu, reportErr); err != nil {
				s.core.pool.Put(buffer)
				reportErr(err)
				break produceLoop
			}
			chunkIndex++
		} else {
			s.core.pool.Put(buffer)
		}

		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			reportErr(fmt.Errorf("read error: %w", readErr))
			break
		}
	}

	workerPool.Close()
	workerPool.Wait()
	return stats, uploadErr
}

// submitChunkReplication schedules one chunk replication task into the worker pool.
func (s *uploadService) submitChunkReplication(
	ctx context.Context,
	workerPool *resilience.WorkerPool,
	fileID string,
	chunkIndex int,
	buffer *[]byte,
	readN int,
	stats *uploadStats,
	statsMu *sync.Mutex,
	reportErr func(error),
) error {
	chunkData := (*buffer)[:readN]
	return workerPool.Submit(ctx, func() {
		defer s.core.pool.Put(buffer)

		if err := s.chunker.ReplicateChunk(ctx, fileID, chunkIndex, chunkData); err != nil {
			reportErr(err)
			return
		}

		statsMu.Lock()
		stats.totalSize += int64(len(chunkData))
		stats.chunkCount++
		statsMu.Unlock()
	})
}

// persistFileMetadata marshals and writes metadata after all chunks are replicated.
func (s *uploadService) persistFileMetadata(ctx context.Context, fileID string, fileName string, stats uploadStats) error {
	meta := domain.FileMetadata{
		FileID:    fileID,
		FileName:  fileName,
		Extension: filepath.Ext(fileName),
		Size:      stats.totalSize,
		Chunks:    stats.chunkCount,
		CreatedAt: time.Now(),
	}

	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaID := buildMetadataID(fileID)
	if err := s.metadata.SaveMetadata(ctx, metaID, metaBytes); err != nil {
		return err
	}
	return nil
}

// cleanupUpload best-effort deletes already-written chunks after failed uploads.
func (s *uploadService) cleanupUpload(fileID string, chunkCount int) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
		defer cancel()

		logger.Infow("Cleanup upload started", "file_id", fileID, "chunks", chunkCount)
		strategy := shard.NewShardStrategy(s.core.ring)

		for i := 0; i < chunkCount; i++ {
			chunkID := buildChunkID(fileID, i)
			token := murmur3.Sum64([]byte(chunkID))
			replicas := strategy.GetReplicas(token, 0)

			for _, node := range replicas {
				if _, err := s.core.storage.DeleteChunk(ctx, node.Addr, string(chunkID)); err != nil {
					logger.Warnw(
						"Cleanup chunk delete failed",
						"file_id", fileID,
						"chunk_id", chunkID,
						"node", node.ID,
						"error", err.Error(),
					)
				}
			}
		}

		logger.Infow("Cleanup upload finished", "file_id", fileID, "chunks", chunkCount)
	}()
}
