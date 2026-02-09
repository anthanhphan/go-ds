package service

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/spaolacci/murmur3"
)

type fakeStorageRepository struct {
	writes int
}

func (f *fakeStorageRepository) WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
	f.writes++
	_, _ = io.Copy(io.Discard, reader)
	return nil
}

func (f *fakeStorageRepository) ReadChunk(ctx context.Context, id domain.ChunkID) (uint32, io.ReadCloser, error) {
	return 0, io.NopCloser(bytes.NewReader(nil)), nil
}

func (f *fakeStorageRepository) HasChunk(ctx context.Context, id domain.ChunkID) (bool, error) {
	return false, nil
}

func (f *fakeStorageRepository) DeleteChunk(ctx context.Context, id domain.ChunkID) error {
	return nil
}

func (f *fakeStorageRepository) ListItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error) {
	return nil, nil
}

func (f *fakeStorageRepository) ListAllKeys() []domain.ChunkID {
	return nil
}

func (f *fakeStorageRepository) GetParentID(id domain.ChunkID) (string, bool) {
	return "", false
}

func (f *fakeStorageRepository) GetMerkleRoot() string {
	return ""
}

func (f *fakeStorageRepository) GetMerkleNodes(indices []int32) ([]shard.MerkleNode, error) {
	return nil, nil
}

func (f *fakeStorageRepository) Compact() error {
	return nil
}

func TestWriteChunk_AcceptsOwnerNode(t *testing.T) {
	repo := &fakeStorageRepository{}
	ring := shard.NewRing(16)
	ring.AddNode(shard.Node{ID: "node-1", Addr: "node-1:8081", ShardID: "shard-a", ReplicaID: 1})
	ring.AddNode(shard.Node{ID: "node-2", Addr: "node-2:8081", ShardID: "shard-a", ReplicaID: 2})

	svc := NewStorageService(repo, nil, ring, "node-1", "shard-a")
	err := svc.WriteChunk(context.Background(), domain.ChunkID("chunk-owner"), "file-1", 0, bytes.NewReader([]byte("payload")))
	if err != nil {
		t.Fatalf("expected owner write success, got error: %v", err)
	}
	if repo.writes != 1 {
		t.Fatalf("expected one write, got %d", repo.writes)
	}
}

func TestWriteChunk_RejectsNonOwnerNode(t *testing.T) {
	repo := &fakeStorageRepository{}
	ring := shard.NewRing(16)
	ring.AddNode(shard.Node{ID: "node-a", Addr: "node-a:8081", ShardID: "shard-a", ReplicaID: 1})
	ring.AddNode(shard.Node{ID: "node-b", Addr: "node-b:8081", ShardID: "shard-b", ReplicaID: 1})

	svc := NewStorageService(repo, nil, ring, "node-a", "shard-a")
	chunkID := findChunkOwnedByShard(t, ring, "shard-b")
	err := svc.WriteChunk(context.Background(), chunkID, "file-1", 0, bytes.NewReader([]byte("payload")))
	if err == nil {
		t.Fatalf("expected non-owner write rejection, got nil")
	}
	if repo.writes != 0 {
		t.Fatalf("expected zero local writes, got %d", repo.writes)
	}
}

func findChunkOwnedByShard(t *testing.T, ring *shard.Ring, shardID string) domain.ChunkID {
	t.Helper()

	strategy := shard.NewShardStrategy(ring)
	for i := 0; i < 10000; i++ {
		id := domain.ChunkID(fmt.Sprintf("chunk-%d", i))
		token := murmur3.Sum64([]byte(id))
		replicas := strategy.GetReplicas(token, 0)
		if len(replicas) == 0 {
			continue
		}
		if replicas[0].ShardID == shardID {
			return id
		}
	}
	t.Fatalf("failed to find chunk owned by shard %s", shardID)
	return ""
}
