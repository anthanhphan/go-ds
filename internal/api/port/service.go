package port

import (
	"context"
	"errors"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
)

var (
	ErrFileNotFound  = errors.New("file not found")
	ErrChunkNotFound = errors.New("chunk not found")
)

// ConsistencyLevel defines the consistency requirement for operations.
type ConsistencyLevel int

const (
	ConsistencyOne    ConsistencyLevel = 1
	ConsistencyQuorum ConsistencyLevel = 2
	ConsistencyAll    ConsistencyLevel = 3
)

// FileService defines the business logic for file operations.
type FileService interface {
	// UploadFile uploads a file and returns the generated file ID.
	UploadFile(ctx context.Context, fileName string, reader io.Reader) (string, error)

	// DownloadFile downloads a file from the distributed storage.
	DownloadFile(ctx context.Context, fileID string, writer io.Writer) error

	// GetFileMetadata retrieves metadata for a file.
	GetFileMetadata(ctx context.Context, fileID string) (*domain.FileMetadata, error)
}
