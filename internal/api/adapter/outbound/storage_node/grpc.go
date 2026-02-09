package storage_node

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/anthanhphan/go-distributed-file-storage/internal/api/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/resilience"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"github.com/anthanhphan/gosdk/logger"
)

type GrpcAdapter struct {
	clients  map[string]storagev1.StorageServiceClient
	conns    map[string]*grpc.ClientConn
	breakers map[string]*resilience.CircuitBreaker
	mu       sync.RWMutex
}

func NewGrpcAdapter() *GrpcAdapter {
	return &GrpcAdapter{
		clients:  make(map[string]storagev1.StorageServiceClient),
		conns:    make(map[string]*grpc.ClientConn),
		breakers: make(map[string]*resilience.CircuitBreaker),
	}
}

// Ensure GrpcAdapter implements port.StorageNode
var _ port.StorageNode = (*GrpcAdapter)(nil)

func (a *GrpcAdapter) WriteChunk(ctx context.Context, addr string, chunkID string, parentID string, checksum uint32, reader io.Reader) (*storagev1.WriteChunkResponse, error) {
	breaker := a.getBreaker(addr)
	var response *storagev1.WriteChunkResponse
	err := breaker.Execute(ctx, func(execCtx context.Context) error {
		client, err := a.getClient(addr)
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}

		stream, err := client.WriteChunk(execCtx)
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}

		buf := make([]byte, 64*1024)
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&storagev1.WriteChunkRequest{
					ChunkId:  chunkID,
					ParentId: parentID,
					Data:     buf[:n],
					Checksum: checksum,
				}); sendErr != nil {
					return normalizeRPCErr(execCtx, sendErr)
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return normalizeRPCErr(execCtx, readErr)
			}
		}

		response, err = stream.CloseAndRecv()
		return normalizeRPCErr(execCtx, err)
	})
	if err != nil {
		a.handleRPCErr(addr, err, "WriteChunk")
		return nil, err
	}
	return response, nil
}

func (a *GrpcAdapter) ReadChunk(ctx context.Context, addr string, chunkID string, writer io.Writer) (*storagev1.ReadChunkResponse, error) {
	breaker := a.getBreaker(addr)
	var lastResp *storagev1.ReadChunkResponse
	err := breaker.Execute(ctx, func(execCtx context.Context) error {
		client, err := a.getClient(addr)
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}

		stream, err := client.ReadChunk(execCtx, &storagev1.ReadChunkRequest{
			ChunkId: chunkID,
		})
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return normalizeRPCErr(execCtx, recvErr)
			}

			if !resp.Found {
				lastResp = resp
				return nil
			}

			lastResp = resp
			if _, writeErr := writer.Write(resp.Data); writeErr != nil {
				return normalizeRPCErr(execCtx, writeErr)
			}
		}
		return nil
	})
	if err != nil {
		a.handleRPCErr(addr, err, "ReadChunk")
		return nil, err
	}
	return lastResp, nil
}

func (a *GrpcAdapter) DeleteChunk(ctx context.Context, addr string, chunkID string) (*storagev1.DeleteChunkResponse, error) {
	breaker := a.getBreaker(addr)
	var response *storagev1.DeleteChunkResponse
	err := breaker.Execute(ctx, func(execCtx context.Context) error {
		client, err := a.getClient(addr)
		if err != nil {
			return normalizeRPCErr(execCtx, err)
		}

		response, err = client.DeleteChunk(execCtx, &storagev1.DeleteChunkRequest{
			ChunkId: chunkID,
		})
		return normalizeRPCErr(execCtx, err)
	})
	if err != nil {
		a.handleRPCErr(addr, err, "DeleteChunk")
		return nil, err
	}
	return response, nil
}

func (a *GrpcAdapter) getClient(addr string) (storagev1.StorageServiceClient, error) {
	a.mu.RLock()
	client, ok := a.clients[addr]
	a.mu.RUnlock()
	if ok {
		return client, nil
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Double check
	if client, ok := a.clients[addr]; ok {
		return client, nil
	}

	// Default 4MB is too small. Set to 16MB.
	maxMsgSize := 16 * 1024 * 1024
	// Modern gRPC recommends NewClient
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		return nil, err
	}

	client = storagev1.NewStorageServiceClient(conn)
	a.clients[addr] = client
	a.conns[addr] = conn

	return client, nil
}

func (a *GrpcAdapter) getBreaker(addr string) *resilience.CircuitBreaker {
	a.mu.RLock()
	cb, ok := a.breakers[addr]
	a.mu.RUnlock()
	if ok {
		return cb
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	if cb, ok = a.breakers[addr]; ok {
		return cb
	}
	cb = resilience.NewCircuitBreaker(resilience.CircuitBreakerConfig{
		Name:              addr,
		FailureThreshold:  3,
		SuccessThreshold:  2,
		OpenTimeout:       10 * time.Second,
		HalfOpenMaxFlight: 5,
	})
	a.breakers[addr] = cb
	return cb
}

func (a *GrpcAdapter) handleRPCErr(addr string, err error, op string) {
	if errors.Is(err, resilience.ErrCircuitOpen) {
		logger.Warnw("Storage RPC short-circuited", "op", op, "addr", addr, "error", err.Error())
		var openErr *resilience.CircuitOpenError
		if errors.As(err, &openErr) && openErr.RetryAfter <= 0 {
			// Force reconnect when breaker is ready to probe immediately.
			a.dropClient(addr)
		}
		return
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return
	}

	logger.Warnw("Storage RPC failed", "op", op, "addr", addr, "error", err.Error())
	a.dropClient(addr)
}

func (a *GrpcAdapter) dropClient(addr string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if conn, ok := a.conns[addr]; ok {
		_ = conn.Close()
		delete(a.conns, addr)
	}
	delete(a.clients, addr)
}

func normalizeRPCErr(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
		return context.Canceled
	}
	// gRPC stream operations can surface EOF after caller canceled the context.
	if errors.Is(err, io.EOF) && ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return context.Canceled
	}
	return err
}

func (a *GrpcAdapter) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, conn := range a.conns {
		_ = conn.Close()
	}
}
