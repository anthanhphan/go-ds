package service

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/idgen"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
)

// FileServiceImpl is the facade that wires use-case services for file operations.
type FileServiceImpl struct {
	cfg     *config.Config
	ring    *shard.Ring
	storage port.StorageNode
	idGen   *idgen.Snowflake
	pool    *sync.Pool

	uploadUseCase   *uploadService
	downloadUseCase *downloadService
	metadataUseCase *metadataService
	chunkWriter     *chunkWriteService
}

// Ensure FileServiceImpl implements port.FileService.
var _ port.FileService = (*FileServiceImpl)(nil)

// NewFileService builds the file service facade and all use-case services.
func NewFileService(cfg *config.Config, ring *shard.Ring, storage port.StorageNode, idGen *idgen.Snowflake) *FileServiceImpl {
	svc := &FileServiceImpl{
		cfg:     cfg,
		ring:    ring,
		storage: storage,
		idGen:   idGen,
		pool: &sync.Pool{
			New: func() interface{} {
				// Allocate one reusable chunk buffer per worker task.
				b := make([]byte, cfg.App.ChunkSize)
				return &b
			},
		},
	}

	svc.chunkWriter = newChunkWriteService(svc)
	svc.metadataUseCase = newMetadataService(svc)
	svc.uploadUseCase = newUploadService(svc, svc.chunkWriter, svc.metadataUseCase, svc.idGen)
	svc.downloadUseCase = newDownloadService(svc, svc.metadataUseCase)

	return svc
}

// UploadFile delegates upload orchestration to the upload use-case service.
func (s *FileServiceImpl) UploadFile(ctx context.Context, fileName string, reader io.Reader) (string, error) {
	return s.uploadUseCase.uploadFile(ctx, fileName, reader)
}

// DownloadFile delegates file reconstruction to the download use-case service.
func (s *FileServiceImpl) DownloadFile(ctx context.Context, fileID string, writer io.Writer) error {
	return s.downloadUseCase.downloadFile(ctx, fileID, writer)
}

// GetFileMetadata delegates metadata read to the metadata use-case service.
func (s *FileServiceImpl) GetFileMetadata(ctx context.Context, fileID string) (*domain.FileMetadata, error) {
	return s.metadataUseCase.getFileMetadata(ctx, fileID)
}

// getReadConsistency resolves configured read consistency with fallback.
func (s *FileServiceImpl) getReadConsistency() port.ConsistencyLevel {
	return s.parseConsistency(s.cfg.App.ReadConsistency)
}

// getWriteConsistency resolves configured write consistency with fallback.
func (s *FileServiceImpl) getWriteConsistency() port.ConsistencyLevel {
	return s.parseConsistency(s.cfg.App.WriteConsistency)
}

// parseConsistency converts string config to internal consistency enum.
func (s *FileServiceImpl) parseConsistency(c string) port.ConsistencyLevel {
	switch c {
	case "one":
		return port.ConsistencyOne
	case "all":
		return port.ConsistencyAll
	case "quorum":
		return port.ConsistencyQuorum
	default:
		logger.Warnw("Unknown consistency level, fallback to quorum", "value", c)
		return port.ConsistencyQuorum
	}
}

// requiredSuccess calculates ACK threshold from consistency and replica count.
func (s *FileServiceImpl) requiredSuccess(level port.ConsistencyLevel, total int) (int, error) {
	required := 1
	switch level {
	case port.ConsistencyQuorum:
		required = (total / 2) + 1
	case port.ConsistencyAll:
		required = total
	}
	if required > total {
		return 0, fmt.Errorf("insufficient replicas: required=%d available=%d", required, total)
	}
	return required, nil
}

// maxRetries returns write retry count with safe default.
func (s *FileServiceImpl) maxRetries() int {
	if s.cfg.App.MaxRetries > 0 {
		return s.cfg.App.MaxRetries
	}
	return 3
}

// writeTimeout returns per-attempt write timeout with safe default.
func (s *FileServiceImpl) writeTimeout() time.Duration {
	if s.cfg.App.WriteTimeoutMS > 0 {
		return time.Duration(s.cfg.App.WriteTimeoutMS) * time.Millisecond
	}
	return 15 * time.Second
}

// replicateChunk keeps backward compatibility for existing unit tests.
func (s *FileServiceImpl) replicateChunk(ctx context.Context, fileID string, chunkIndex int, chunkData []byte) error {
	return s.chunkWriter.ReplicateChunk(ctx, fileID, chunkIndex, chunkData)
}
