package service

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/service/mocks"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"go.uber.org/mock/gomock"
)

func TestGarbageCollectorService_GarbageCollect(t *testing.T) {
	type mockSetup func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort)

	tests := []struct {
		name             string
		setupMocks       mockSetup
		wantDeletedCount int32
		wantErr          bool
	}{
		{
			name: "RemovesOrphans",
			setupMocks: func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort) {
				// LIST: Found chunk1
				repo.EXPECT().ListAllKeys().Return([]domain.ChunkID{"chunk1"})
				// PARENT: chunk1 -> file1
				repo.EXPECT().GetParentID(domain.ChunkID("chunk1")).Return("file1", true)
				// LOCAL META CHECK: meta:file1 -> NOT FOUND
				repo.EXPECT().HasChunk(gomock.Any(), domain.ChunkID("meta:file1")).Return(false, nil)
				// REMOTE META CHECK: meta:file1 -> NOT FOUND (orphaned)
				repl.EXPECT().FetchChunk(gomock.Any(), gomock.Any(), "meta:file1").
					Return(uint32(0), nil, port.ErrChunkNotFound).
					AnyTimes()

				// READ (estimate size)
				repo.EXPECT().ReadChunk(gomock.Any(), domain.ChunkID("chunk1")).
					Return(uint32(100), io.NopCloser(bytes.NewReader(make([]byte, 100))), nil)
				// DELETE: chunk1
				repo.EXPECT().DeleteChunk(gomock.Any(), domain.ChunkID("chunk1")).Return(nil)
				// COMPACT: Triggered
				repo.EXPECT().Compact().Return(nil).MaxTimes(1)
			},
			wantDeletedCount: 1,
			wantErr:          false,
		},
		{
			name: "KeepsActiveChunks",
			setupMocks: func(repo *mocks.MockStorageRepository, repl *mocks.MockReplicationPort) {
				// LIST: Found chunk1
				repo.EXPECT().ListAllKeys().Return([]domain.ChunkID{"chunk1"})
				// PARENT: chunk1 -> file1
				repo.EXPECT().GetParentID(domain.ChunkID("chunk1")).Return("file1", true)
				// LOCAL META CHECK: meta:file1 -> FOUND (active)
				repo.EXPECT().HasChunk(gomock.Any(), domain.ChunkID("meta:file1")).Return(true, nil)

				// DELETE: Should NOT be called
				repo.EXPECT().DeleteChunk(gomock.Any(), gomock.Any()).Times(0)
			},
			wantDeletedCount: 0,
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			mockRepo := mocks.NewMockStorageRepository(ctrl)
			mockRepl := mocks.NewMockReplicationPort(ctrl)

			if tt.setupMocks != nil {
				tt.setupMocks(mockRepo, mockRepl)
			}

			// Setup Ring with multiple nodes to ensure remote logic is exercisable
			ring := shard.NewRing(10)
			ring.AddNode(shard.Node{ID: "node1", Addr: "addr1"})
			ring.AddNode(shard.Node{ID: "node2", Addr: "addr2"})

			svc := newGarbageCollectorService(&StorageServiceImpl{
				storage:     mockRepo,
				replication: mockRepl,
				ring:        ring,
				nodeID:      "node1",
			})

			deleted, _, err := svc.garbageCollect(context.Background(), 0)
			if (err != nil) != tt.wantErr {
				t.Errorf("garbageCollect() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if deleted != tt.wantDeletedCount {
				t.Errorf("garbageCollect() deleted = %v, want %v", deleted, tt.wantDeletedCount)
			}
		})
	}
}
