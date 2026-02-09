package grpc_handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/resilience"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"github.com/anthanhphan/gosdk/logger"
)

// ClientAdapter implements port.ReplicationPort.
type ClientAdapter struct {
	mu       sync.RWMutex
	conns    map[string]*grpc.ClientConn
	breakers map[string]*resilience.CircuitBreaker
}

// NewClientAdapter creates a new replication client.
func NewClientAdapter() *ClientAdapter {
	return &ClientAdapter{
		conns:    make(map[string]*grpc.ClientConn),
		breakers: make(map[string]*resilience.CircuitBreaker),
	}
}

// Ensure ClientAdapter implements ReplicationPort
var _ port.ReplicationPort = (*ClientAdapter)(nil)

func (c *ClientAdapter) getConn(addr string) (*grpc.ClientConn, error) {
	c.mu.RLock()
	conn, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return conn, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Double check
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	// For now, use insecure credentials.
	// In prod, mTLS would be configured here.
	newConn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	c.conns[addr] = newConn
	return newConn, nil
}

// ReplicateChunk sends a chunk to a target node.
func (c *ClientAdapter) ReplicateChunk(ctx context.Context, target shard.Node, chunk domain.Chunk) error {
	// Set a timeout for replication if not already set in context
	repCtx, cancel := c.withDefaultTimeout(ctx, 30*time.Second)
	defer cancel()

	return c.withBreaker(repCtx, target.Addr, "ReplicateChunk", func(callCtx context.Context, client storagev1.StorageServiceClient) error {
		stream, err := client.WriteChunk(callCtx)
		if err != nil {
			return fmt.Errorf("failed to create write stream: %w", err)
		}

		if err := stream.Send(&storagev1.WriteChunkRequest{
			ChunkId:  string(chunk.ID),
			Data:     chunk.Data,
			Checksum: chunk.Checksum,
		}); err != nil {
			return fmt.Errorf("failed to send chunk data: %w", err)
		}

		resp, err := stream.CloseAndRecv()
		if err != nil {
			return fmt.Errorf("rpc failed: %w", err)
		}

		if !resp.Success {
			return fmt.Errorf("remote write failed: %s", resp.Message)
		}

		return nil
	})
}

func (c *ClientAdapter) GetMerkleRoot(ctx context.Context, targetAddr string) (string, error) {
	callCtx, cancel := c.withDefaultTimeout(ctx, 3*time.Second)
	defer cancel()

	var root string
	err := c.withBreaker(callCtx, targetAddr, "GetMerkleRoot", func(execCtx context.Context, client storagev1.StorageServiceClient) error {
		resp, err := client.GetMerkleRoot(execCtx, &storagev1.GetMerkleRootRequest{})
		if err != nil {
			return err
		}
		root = resp.RootHash
		return nil
	})
	return root, err
}

func (c *ClientAdapter) GetMerkleNodes(ctx context.Context, targetAddr string, indices []int32) ([]shard.MerkleNode, error) {
	callCtx, cancel := c.withDefaultTimeout(ctx, 5*time.Second)
	defer cancel()

	var nodes []shard.MerkleNode
	err := c.withBreaker(callCtx, targetAddr, "GetMerkleNodes", func(execCtx context.Context, client storagev1.StorageServiceClient) error {
		resp, err := client.GetMerkleNodes(execCtx, &storagev1.GetMerkleNodesRequest{Indices: indices})
		if err != nil {
			return err
		}

		nodes = make([]shard.MerkleNode, 0, len(resp.Nodes))
		for _, n := range resp.Nodes {
			nodes = append(nodes, shard.MerkleNode{
				Index:     n.Index,
				Hash:      n.Hash,
				LeftHash:  n.LeftHash,
				RightHash: n.RightHash,
			})
		}
		return nil
	})
	return nodes, err
}

func (c *ClientAdapter) ListItemsInBucket(ctx context.Context, targetAddr string, bucketID int32) ([]shard.BucketItem, error) {
	callCtx, cancel := c.withDefaultTimeout(ctx, 5*time.Second)
	defer cancel()

	var items []shard.BucketItem
	err := c.withBreaker(callCtx, targetAddr, "ListItemsInBucket", func(execCtx context.Context, client storagev1.StorageServiceClient) error {
		resp, err := client.ListItemsInBucket(execCtx, &storagev1.ListItemsInBucketRequest{BucketId: bucketID})
		if err != nil {
			return err
		}

		items = make([]shard.BucketItem, 0, len(resp.Items))
		for _, item := range resp.Items {
			items = append(items, shard.BucketItem{
				ChunkID:  item.ChunkId,
				Checksum: item.Checksum,
			})
		}
		return nil
	})
	return items, err
}

func (c *ClientAdapter) FetchChunk(ctx context.Context, targetAddr string, chunkID string) (uint32, io.ReadCloser, error) {
	callCtx, cancel := c.withDefaultTimeout(ctx, 10*time.Second)
	defer cancel()

	var (
		checksum uint32
		reader   io.ReadCloser
	)
	err := c.withBreaker(callCtx, targetAddr, "FetchChunk", func(execCtx context.Context, client storagev1.StorageServiceClient) error {
		stream, err := client.ReadChunk(execCtx, &storagev1.ReadChunkRequest{ChunkId: chunkID})
		if err != nil {
			return err
		}

		firstResp, err := stream.Recv()
		if err == io.EOF {
			return port.ErrChunkNotFound
		}
		if err != nil {
			return err
		}
		if !firstResp.Found {
			return port.ErrChunkNotFound
		}

		checksum = firstResp.Checksum
		pr, pw := io.Pipe()

		go func() {
			defer func() { _ = pw.Close() }()

			if len(firstResp.Data) > 0 {
				if _, err := pw.Write(firstResp.Data); err != nil {
					return
				}
			}

			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = pw.CloseWithError(err)
					return
				}
				if !resp.Found {
					_ = pw.CloseWithError(port.ErrChunkNotFound)
					return
				}
				if _, err := pw.Write(resp.Data); err != nil {
					return
				}
			}
		}()

		reader = pr
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	return checksum, reader, nil
}

func (c *ClientAdapter) withBreaker(ctx context.Context, addr, op string, fn func(context.Context, storagev1.StorageServiceClient) error) error {
	breaker := c.getBreaker(addr)
	err := breaker.Execute(ctx, func(execCtx context.Context) error {
		conn, err := c.getConn(addr)
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}
		client := storagev1.NewStorageServiceClient(conn)
		return normalizeRPCErr(execCtx, fn(execCtx, client))
	})
	if err == nil {
		return nil
	}
	if errors.Is(err, resilience.ErrCircuitOpen) {
		logger.Warnw("Replication RPC short-circuited", "op", op, "target", addr, "error", err.Error())
		return err
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	logger.Warnw("Replication RPC failed", "op", op, "target", addr, "error", err.Error())
	c.dropConn(addr)
	return err
}

func (c *ClientAdapter) withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (c *ClientAdapter) getBreaker(addr string) *resilience.CircuitBreaker {
	c.mu.RLock()
	cb, ok := c.breakers[addr]
	c.mu.RUnlock()
	if ok {
		return cb
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if cb, ok = c.breakers[addr]; ok {
		return cb
	}
	cb = resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{
		Name:              addr,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		OpenTimeout:       10 * time.Second,
		HalfOpenMaxFlight: 1,
	})
	c.breakers[addr] = cb
	return cb
}

func (c *ClientAdapter) dropConn(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[addr]; ok {
		_ = conn.Close()
		delete(c.conns, addr)
	}
}

// Close closes all connections.
func (c *ClientAdapter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
	return nil
}

func normalizeRPCErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return context.Canceled
	}
	if errors.Is(err, io.EOF) && ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return err
}
