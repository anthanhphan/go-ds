package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/resilience"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
)

type fakeStorageNode struct {
	writeFn func(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error)
}

func (f *fakeStorageNode) WriteChunk(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error) {
	return f.writeFn(ctx, addr, chunkID, parentID, checksum, reader)
}

func (f *fakeStorageNode) ReadChunk(ctx context.Context, addr string, chunkID string, writer io.Writer) (*storagev1.ReadChunkResponse, error) {
	return nil, errors.New("not implemented")
}

func (f *fakeStorageNode) DeleteChunk(ctx context.Context, addr string, chunkID string) (*storagev1.DeleteChunkResponse, error) {
	return nil, errors.New("not implemented")
}

func TestReplicateChunkWriteTimeoutConfig(t *testing.T) {
	makeService := func(writeTimeoutMS int) *FileServiceImpl {
		cfg := config.DefaultConfig()
		cfg.App.WriteConsistency = "quorum"
		cfg.App.MaxRetries = 1
		cfg.App.WriteTimeoutMS = writeTimeoutMS

		ring := shard.NewRing(16)
		ring.AddNode(shard.Node{ID: "storage-node-1", Addr: "storage-1:8081", ShardID: "shard-1", ReplicaID: 1})
		ring.AddNode(shard.Node{ID: "storage-node-2", Addr: "storage-2:8081", ShardID: "shard-1", ReplicaID: 2})
		ring.AddNode(shard.Node{ID: "storage-node-3", Addr: "storage-3:8081", ShardID: "shard-1", ReplicaID: 3})

		storage := &fakeStorageNode{
			writeFn: func(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error) {
				_, _ = io.Copy(io.Discard, reader)
				switch addr {
				case "storage-1:8081":
					return &storagev1.WriteChunkResponse{Success: true}, nil
				case "storage-2:8081":
					select {
					case <-time.After(500 * time.Millisecond):
						return &storagev1.WriteChunkResponse{Success: true}, nil
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				case "storage-3:8081":
					return nil, fmt.Errorf("%w for %s", resilience.ErrCircuitOpen, addr)
				default:
					return nil, fmt.Errorf("unexpected addr: %s", addr)
				}
			},
		}

		return NewFileService(cfg, ring, storage, nil)
	}

	t.Run("timeout too low fails quorum", func(t *testing.T) {
		svc := makeService(200)
		err := svc.replicateChunk(context.Background(), "file-1", 3, []byte("payload"))
		if err == nil {
			t.Fatalf("expected quorum failure, got nil")
		}
		if !strings.Contains(err.Error(), "consistency failed") {
			t.Fatalf("expected consistency failure, got: %v", err)
		}
	})

	t.Run("timeout high enough passes quorum", func(t *testing.T) {
		svc := makeService(1000)
		if err := svc.replicateChunk(context.Background(), "file-1", 3, []byte("payload")); err != nil {
			t.Fatalf("expected success, got error: %v", err)
		}
	})
}

func TestRequiredSuccessUsesTotalReplicas(t *testing.T) {
	cfg := config.DefaultConfig()
	svc := &FileServiceImpl{cfg: cfg}

	cases := []struct {
		name     string
		level    port.ConsistencyLevel
		total    int
		expected int
	}{
		{name: "one", level: port.ConsistencyOne, total: 2, expected: 1},
		{name: "quorum on 2 replicas", level: port.ConsistencyQuorum, total: 2, expected: 2},
		{name: "quorum on 3 replicas", level: port.ConsistencyQuorum, total: 3, expected: 2},
		{name: "all", level: port.ConsistencyAll, total: 2, expected: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			required, err := svc.requiredSuccess(tc.level, tc.total)
			if err != nil {
				t.Fatalf("requiredSuccess returned error: %v", err)
			}
			if required != tc.expected {
				t.Fatalf("requiredSuccess = %d, expected %d", required, tc.expected)
			}
		})
	}
}

func TestReplicateChunk_DoesNotCancelInflightReplicaOnQuorumSuccess(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.App.WriteConsistency = "quorum"
	cfg.App.MaxRetries = 1
	cfg.App.WriteTimeoutMS = 500

	ring := shard.NewRing(16)
	ring.AddNode(shard.Node{ID: "storage-node-1", Addr: "storage-1:8081", ShardID: "shard-1", ReplicaID: 1})
	ring.AddNode(shard.Node{ID: "storage-node-2", Addr: "storage-2:8081", ShardID: "shard-1", ReplicaID: 2})
	ring.AddNode(shard.Node{ID: "storage-node-3", Addr: "storage-3:8081", ShardID: "shard-1", ReplicaID: 3})

	thirdReplicaErr := make(chan error, 1)
	storage := &fakeStorageNode{
		writeFn: func(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error) {
			_, _ = io.Copy(io.Discard, reader)

			switch addr {
			case "storage-1:8081", "storage-2:8081":
				return &storagev1.WriteChunkResponse{Success: true}, nil
			case "storage-3:8081":
				select {
				case <-ctx.Done():
					thirdReplicaErr <- ctx.Err()
					return nil, ctx.Err()
				case <-time.After(300 * time.Millisecond):
					thirdReplicaErr <- nil
					return &storagev1.WriteChunkResponse{Success: true}, nil
				}
			default:
				return nil, fmt.Errorf("unexpected addr: %s", addr)
			}
		},
	}

	svc := NewFileService(cfg, ring, storage, nil)
	if err := svc.replicateChunk(context.Background(), "file-1", 1, []byte("payload")); err != nil {
		t.Fatalf("expected quorum success, got %v", err)
	}

	select {
	case err := <-thirdReplicaErr:
		if errors.Is(err, context.Canceled) {
			t.Fatalf("third replica was canceled on quorum success")
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("timed out waiting for third replica result")
	}
}

func TestReplicateChunk_RetriesWhenCircuitOpenRetryZero(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.App.WriteConsistency = "quorum"
	cfg.App.MaxRetries = 3
	cfg.App.WriteTimeoutMS = 1000

	ring := shard.NewRing(16)
	ring.AddNode(shard.Node{ID: "storage-node-1", Addr: "storage-1:8081", ShardID: "shard-1", ReplicaID: 1})
	ring.AddNode(shard.Node{ID: "storage-node-2", Addr: "storage-2:8081", ShardID: "shard-1", ReplicaID: 2})
	ring.AddNode(shard.Node{ID: "storage-node-3", Addr: "storage-3:8081", ShardID: "shard-1", ReplicaID: 3})

	var mu sync.Mutex
	attempts := map[string]int{}
	storage := &fakeStorageNode{
		writeFn: func(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error) {
			_, _ = io.Copy(io.Discard, reader)

			mu.Lock()
			attempts[addr]++
			currentAttempt := attempts[addr]
			mu.Unlock()

			if addr == "storage-2:8081" && currentAttempt == 1 {
				return nil, &resilience.CircuitOpenError{
					Name:       addr,
					RetryAfter: 0,
				}
			}
			if addr == "storage-3:8081" {
				return nil, errors.New("down")
			}
			return &storagev1.WriteChunkResponse{Success: true}, nil
		},
	}

	svc := NewFileService(cfg, ring, storage, nil)
	if err := svc.replicateChunk(context.Background(), "file-1", 2, []byte("payload")); err != nil {
		t.Fatalf("expected success after circuit-open retry, got %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts["storage-2:8081"] < 2 {
		t.Fatalf("expected retried attempt on storage-2, got %d", attempts["storage-2:8081"])
	}
}
