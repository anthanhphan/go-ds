package service

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/service/mocks"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"go.uber.org/mock/gomock"
)

// mockPool helper
func newMockPool() *sync.Pool {
	return &sync.Pool{
		New: func() interface{} {
			b := make([]byte, 1024)
			return &b
		},
	}
}

func TestUploadService_UploadFile(t *testing.T) {
	type mockSetup func(chunker *mocks.MockChunkReplicator, metadata *mocks.MockMetadataPersistence, idGen *mocks.MockIDGenerator, storage *mocks.MockStorageNode)

	tests := []struct {
		name        string
		fileContent []byte
		setup       mockSetup
		wantErr     bool
		errContains string
	}{
		{
			name:        "Success",
			fileContent: []byte("chunk1-data-chunk2-data"),
			setup: func(chunker *mocks.MockChunkReplicator, metadata *mocks.MockMetadataPersistence, idGen *mocks.MockIDGenerator, storage *mocks.MockStorageNode) {
				// ID Gen
				idGen.EXPECT().Next().Return(int64(12345), nil)

				// ReplicateChunk: Expect calls. Number depends on chunk size vs buffer size.
				// Test buffer in core is 1024. Data is very small. So 1 chunk.
				chunker.EXPECT().
					ReplicateChunk(gomock.Any(), "12345", gomock.Any(), gomock.Any()).
					Return(nil)

				// SaveMetadata
				metadata.EXPECT().
					SaveMetadata(gomock.Any(), "meta:12345", gomock.Any()).
					Return(nil)
			},
			wantErr: false,
		},
		{
			name:        "ChunkFailure",
			fileContent: []byte("data"),
			setup: func(chunker *mocks.MockChunkReplicator, metadata *mocks.MockMetadataPersistence, idGen *mocks.MockIDGenerator, storage *mocks.MockStorageNode) {
				idGen.EXPECT().Next().Return(int64(12345), nil)

				// ReplicateChunk -> Failure
				chunker.EXPECT().
					ReplicateChunk(gomock.Any(), "12345", gomock.Any(), gomock.Any()).
					Return(errors.New("replication failed"))

				// Cleanup: DeleteChunk might be called
				storage.EXPECT().
					DeleteChunk(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&storagev1.DeleteChunkResponse{Success: true}, nil).
					AnyTimes()
			},
			wantErr:     true,
			errContains: "replication failed",
		},
		{
			name:        "MetadataSaveFailure",
			fileContent: []byte("data"),
			setup: func(chunker *mocks.MockChunkReplicator, metadata *mocks.MockMetadataPersistence, idGen *mocks.MockIDGenerator, storage *mocks.MockStorageNode) {
				idGen.EXPECT().Next().Return(int64(12345), nil)
				chunker.EXPECT().ReplicateChunk(gomock.Any(), "12345", gomock.Any(), gomock.Any()).Return(nil)

				// SaveMetadata -> Failure
				metadata.EXPECT().
					SaveMetadata(gomock.Any(), "meta:12345", gomock.Any()).
					Return(errors.New("db error"))

				// Cleanup triggered
				storage.EXPECT().
					DeleteChunk(gomock.Any(), gomock.Any(), gomock.Any()).
					Return(&storagev1.DeleteChunkResponse{Success: true}, nil).
					AnyTimes()
			},
			wantErr:     true,
			errContains: "db error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockIDGen := mocks.NewMockIDGenerator(ctrl)
			mockChunker := mocks.NewMockChunkReplicator(ctrl)
			mockMetadata := mocks.NewMockMetadataPersistence(ctrl)
			mockStorage := mocks.NewMockStorageNode(ctrl)

			if tt.setup != nil {
				tt.setup(mockChunker, mockMetadata, mockIDGen, mockStorage)
			}

			ring := shard.NewRing(10)

			core := &FileServiceImpl{
				cfg: &config.Config{
					App: config.AppConfig{
						ParallelChunks: 2,
					},
				},
				pool:    newMockPool(),
				ring:    ring,
				storage: mockStorage,
			}

			svc := newUploadService(core, mockChunker, mockMetadata, mockIDGen)

			fileID, err := svc.uploadFile(context.Background(), "test.txt", bytes.NewReader(tt.fileContent))

			if (err != nil) != tt.wantErr {
				t.Errorf("uploadFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil && tt.errContains != "" {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error message %v does not contain %v", err, tt.errContains)
				}
			}
			if !tt.wantErr && fileID != "12345" {
				t.Errorf("expected fileID 12345, got %s", fileID)
			}
		})
	}
}
