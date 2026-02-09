package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/service/mocks"
	"go.uber.org/mock/gomock"
)

func TestSyncChunkService_SyncChunk(t *testing.T) {
	type mockSetup func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort)

	tests := []struct {
		name    string
		chunkID string
		source  string
		setup   mockSetup
		wantErr bool
	}{
		{
			name:    "AlreadyExists",
			chunkID: "chunk-1",
			source:  "remote-peer:8080",
			setup: func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort) {
				// HasChunk -> true
				repo.EXPECT().
					HasChunk(gomock.Any(), domain.ChunkID("chunk-1")).
					Return(true, nil)
				// FetchChunk -> 0 calls
				repl.EXPECT().
					FetchChunk(gomock.Any(), gomock.Any(), gomock.Any()).
					Times(0)
			},
			wantErr: false,
		},
		{
			name:    "FetchAndStore_Success",
			chunkID: "chunk-1",
			source:  "remote-peer:8080",
			setup: func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort) {
				expectedData := []byte("test-payload")
				// HasChunk -> false
				repo.EXPECT().
					HasChunk(gomock.Any(), domain.ChunkID("chunk-1")).
					Return(false, nil)

				// FetchChunk -> success
				repl.EXPECT().
					FetchChunk(gomock.Any(), "remote-peer:8080", "chunk-1").
					Return(uint32(12345), io.NopCloser(bytes.NewReader(expectedData)), nil)

				// WriteChunk -> success
				repo.EXPECT().
					WriteChunk(gomock.Any(), domain.ChunkID("chunk-1"), "", uint32(12345), gomock.Any()).
					DoAndReturn(func(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
						data, _ := io.ReadAll(reader)
						if string(data) != string(expectedData) {
							return fmt.Errorf("data mismatch")
						}
						return nil
					})
			},
			wantErr: false,
		},
		{
			name:    "FetchFailure",
			chunkID: "chunk-1",
			source:  "remote-peer:8080",
			setup: func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort) {
				// HasChunk -> false
				repo.EXPECT().
					HasChunk(gomock.Any(), domain.ChunkID("chunk-1")).
					Return(false, nil)

				// FetchChunk -> error
				repl.EXPECT().
					FetchChunk(gomock.Any(), "remote-peer:8080", "chunk-1").
					Return(uint32(0), nil, fmt.Errorf("network error"))
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := mocks.NewMockStorageRepository(ctrl)
			mockRepl := mocks.NewMockReplicationPort(ctrl)

			if tt.setup != nil {
				tt.setup(mockRepo, mockRepl)
			}

			core := &StorageServiceImpl{
				storage:     mockRepo,
				replication: mockRepl,
			}
			svc := newSyncChunkService(core)

			err := svc.syncChunk(context.Background(), tt.chunkID, tt.source)
			if (err != nil) != tt.wantErr {
				t.Errorf("syncChunk() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && err != nil {
				// Optional: check error message content if needed
				if strings.Contains(tt.name, "FetchFailure") && !strings.Contains(err.Error(), "failed to fetch chunk") {
					t.Errorf("unexpected error message: %v", err)
				}
			}
		})
	}
}
