package grpc_handler

import (
	"context"
	"io"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"github.com/anthanhphan/gosdk/logger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements the gRPC StorageService.
type Server struct {
	storagev1.UnimplementedStorageServiceServer
	service port.StorageService
}

// NewServer creates a new gRPC server.
func NewServer(service port.StorageService) *Server {
	return &Server{
		service: service,
	}
}

// WriteChunk handles chunk write streaming requests.
func (s *Server) WriteChunk(stream storagev1.StorageService_WriteChunkServer) error {
	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	var chunkID string
	var parentID string
	var checksum uint32

	errChan := make(chan error, 1)
	done := make(chan struct{})

	// Wait for the first message to get chunkID, parentID and checksum
	firstReq, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to read first chunk message: %v", err)
	}
	chunkID = firstReq.ChunkId
	parentID = firstReq.ParentId
	checksum = firstReq.Checksum
	if chunkID == "" {
		return status.Error(codes.InvalidArgument, "chunk id is required")
	}

	go func() {
		defer close(done)
		defer func() { _ = pw.Close() }()
		// Write first chunk data
		if _, err := pw.Write(firstReq.Data); err != nil {
			select {
			case errChan <- err:
			default:
			}
			return
		}
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				select {
				case errChan <- err:
				default:
				}
				return
			}
			if _, err := pw.Write(req.Data); err != nil {
				select {
				case errChan <- err:
				default:
				}
				return
			}
		}
	}()

	if err := s.service.WriteChunk(stream.Context(), domain.ChunkID(chunkID), parentID, checksum, pr); err != nil {
		_ = pr.Close()
		logger.Warnw("WriteChunk rejected", "chunk_id", chunkID, "error", err.Error())
		return stream.SendAndClose(&storagev1.WriteChunkResponse{
			Success: false,
			Message: err.Error(),
		})
	}

	select {
	case err := <-errChan:
		return status.Errorf(codes.Internal, "streaming write failed: %v", err)
	case <-done:
	}

	return stream.SendAndClose(&storagev1.WriteChunkResponse{
		Success: true,
	})
}

// ReadChunk handles chunk read streaming requests.
func (s *Server) ReadChunk(req *storagev1.ReadChunkRequest, stream storagev1.StorageService_ReadChunkServer) error {
	checksum, reader, err := s.service.ReadChunk(stream.Context(), domain.ChunkID(req.ChunkId))
	if err != nil {
		if err == port.ErrChunkNotFound {
			return stream.Send(&storagev1.ReadChunkResponse{Found: false})
		}
		return err
	}
	defer func() { _ = reader.Close() }()

	buf := make([]byte, 64*1024) // 64KB buffer
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if err := stream.Send(&storagev1.ReadChunkResponse{
				ChunkId:  req.ChunkId,
				Data:     buf[:n],
				Checksum: checksum,
				Found:    true,
			}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}

	return nil
}

// DeleteChunk handles chunk deletion.
func (s *Server) DeleteChunk(ctx context.Context, req *storagev1.DeleteChunkRequest) (*storagev1.DeleteChunkResponse, error) {
	if err := s.service.DeleteChunk(ctx, domain.ChunkID(req.ChunkId)); err != nil {
		return &storagev1.DeleteChunkResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}
	return &storagev1.DeleteChunkResponse{
		Success: true,
	}, nil
}

// GetClusterTopology returns the current cluster topology.
func (s *Server) GetClusterTopology(ctx context.Context, req *storagev1.GetClusterTopologyRequest) (*storagev1.GetClusterTopologyResponse, error) {
	nodes, err := s.service.GetClusterTopology(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get topology: %v", err)
	}

	pbNodes := make([]*storagev1.GetClusterTopologyResponse_Node, 0, len(nodes))
	for _, n := range nodes {
		pbNodes = append(pbNodes, &storagev1.GetClusterTopologyResponse_Node{
			Id:      n.ID,
			Addr:    n.Addr,
			ShardId: n.ShardID,
			// G115: Helper safe cast or ignore if we know range is safe. ReplicaID is unlikely to overflow int32.
			ReplicaId: int32(n.ReplicaID), // #nosec G115
		})
	}

	return &storagev1.GetClusterTopologyResponse{
		Nodes: pbNodes,
	}, nil
}

func (s *Server) GetMerkleRoot(ctx context.Context, req *storagev1.GetMerkleRootRequest) (*storagev1.GetMerkleRootResponse, error) {
	root, err := s.service.GetMerkleRoot(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get merkle root: %v", err)
	}
	return &storagev1.GetMerkleRootResponse{RootHash: root}, nil
}

func (s *Server) GetMerkleNodes(ctx context.Context, req *storagev1.GetMerkleNodesRequest) (*storagev1.GetMerkleNodesResponse, error) {
	nodes, err := s.service.GetMerkleNodes(ctx, req.Indices)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get merkle nodes: %v", err)
	}

	pbNodes := make([]*storagev1.GetMerkleNodesResponse_Node, 0, len(nodes))
	for _, n := range nodes {
		pbNodes = append(pbNodes, &storagev1.GetMerkleNodesResponse_Node{
			Index:     n.Index,
			Hash:      n.Hash,
			LeftHash:  n.LeftHash,
			RightHash: n.RightHash,
		})
	}
	return &storagev1.GetMerkleNodesResponse{Nodes: pbNodes}, nil
}

func (s *Server) ListItemsInBucket(ctx context.Context, req *storagev1.ListItemsInBucketRequest) (*storagev1.ListItemsInBucketResponse, error) {
	items, err := s.service.ListItemsInBucket(ctx, req.BucketId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to list bucket items: %v", err)
	}

	pbItems := make([]*storagev1.ListItemsInBucketResponse_Item, 0, len(items))
	for _, item := range items {
		pbItems = append(pbItems, &storagev1.ListItemsInBucketResponse_Item{
			ChunkId:  item.ChunkID,
			Checksum: item.Checksum,
		})
	}
	return &storagev1.ListItemsInBucketResponse{Items: pbItems}, nil
}

func (s *Server) SyncChunk(ctx context.Context, req *storagev1.SyncChunkRequest) (*storagev1.SyncChunkResponse, error) {
	if err := s.service.SyncChunk(ctx, req.ChunkId, req.SourceAddr); err != nil {
		return &storagev1.SyncChunkResponse{
			Success: false,
			Message: err.Error(),
		}, nil
	}
	return &storagev1.SyncChunkResponse{Success: true}, nil
}

func (s *Server) GarbageCollect(ctx context.Context, req *storagev1.GarbageCollectRequest) (*storagev1.GarbageCollectResponse, error) {
	deleted, reclaimed, err := s.service.GarbageCollect(ctx, req.GracePeriodSeconds)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to run garbage collection: %v", err)
	}
	return &storagev1.GarbageCollectResponse{
		ChunksDeleted:  deleted,
		BytesReclaimed: reclaimed,
	}, nil
}
