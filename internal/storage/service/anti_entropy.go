package service

import (
	"context"
	"io"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

const (
	merkleLeafCount  = 1024
	merkleLeafOffset = merkleLeafCount - 1
)

// antiEntropyService reconciles divergent replicas within and across shards.
type antiEntropyService struct {
	core *StorageServiceImpl
}

// newAntiEntropyService creates anti-entropy use-case service.
func newAntiEntropyService(core *StorageServiceImpl) *antiEntropyService {
	return &antiEntropyService{core: core}
}

// startWorker runs periodic anti-entropy reconciliation until context cancellation.
func (s *antiEntropyService) startWorker(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runReconciliation(ctx)
		}
	}
}

// runReconciliation reconciles with every known peer from the ring.
func (s *antiEntropyService) runReconciliation(ctx context.Context) {
	peers := s.core.ring.GetNodes()
	for _, peer := range peers {
		if peer.ID == s.core.nodeID {
			continue
		}
		s.reconcilePeer(ctx, peer)
	}
}

// reconcilePeer picks intra-shard Merkle flow or cross-shard handoff flow.
func (s *antiEntropyService) reconcilePeer(ctx context.Context, peer shard.Node) {
	if peer.ShardID == s.core.shardID {
		s.runIntraShardReconciliation(ctx, peer)
		return
	}
	s.runCrossShardReconciliation(ctx, peer)
}

// runIntraShardReconciliation compares Merkle roots and drills down mismatches.
func (s *antiEntropyService) runIntraShardReconciliation(ctx context.Context, peer shard.Node) {
	logger.Debugw("AE: Starting intra-shard Merkle check", "peer", peer.ID)

	peerRoot, err := s.core.replication.GetMerkleRoot(ctx, peer.Addr)
	if err != nil {
		logger.Warnw("AE: failed to get peer root", "peer", peer.ID, "error", err)
		return
	}

	localRoot, err := s.core.topology.getMerkleRoot(ctx)
	if err != nil {
		logger.Warnw("AE: failed to get local root", "peer", peer.ID, "error", err)
		return
	}
	if peerRoot == localRoot {
		logger.Debugw("AE: Roots match, state in sync", "peer", peer.ID)
		return
	}

	s.reconcileMismatchedNode(ctx, peer.Addr, 0)
}

// runCrossShardReconciliation scans peer data and pulls chunks this node should own.
func (s *antiEntropyService) runCrossShardReconciliation(ctx context.Context, peer shard.Node) {
	logger.Debugw("AE: Starting cross-shard handoff scan", "peer", peer.ID)

	peerItems, err := s.core.replication.ListItemsInBucket(ctx, peer.Addr, -1)
	if err != nil {
		logger.Warnw("AE: failed to list handoff items", "peer", peer.ID, "error", err)
		return
	}

	for _, item := range peerItems {
		if !s.isLocalReplicaOwner(item.ChunkID) {
			continue
		}

		exists, _ := s.core.storage.HasChunk(ctx, domain.ChunkID(item.ChunkID))
		if exists {
			continue
		}

		logger.Infow("AE: Cross-shard missing chunk found, pulling", "chunkID", item.ChunkID, "source", peer.ID)
		if err := s.core.syncer.syncChunk(ctx, item.ChunkID, peer.Addr); err != nil {
			logger.Warnw("AE: Failed to pull handoff chunk", "chunkID", item.ChunkID, "error", err)
		}
	}
}

// reconcileMismatchedNode recursively drills down divergent Merkle subtrees.
func (s *antiEntropyService) reconcileMismatchedNode(ctx context.Context, peerAddr string, idx int32) {
	peerNodes, err := s.core.replication.GetMerkleNodes(ctx, peerAddr, []int32{idx})
	if err != nil || len(peerNodes) == 0 {
		return
	}
	localNodes, err := s.core.topology.getMerkleNodes(ctx, []int32{idx})
	if err != nil || len(localNodes) == 0 {
		return
	}

	peerNode := peerNodes[0]
	localNode := localNodes[0]

	leftIdx := 2*idx + 1
	rightIdx := 2*idx + 2

	if localNode.LeftHash != peerNode.LeftHash {
		if localNode.LeftHash != "" && peerNode.LeftHash != "" {
			s.reconcileMismatchedNode(ctx, peerAddr, leftIdx)
		} else if peerNode.LeftHash != "" {
			s.handlePotentialLeafMismatch(ctx, peerAddr, leftIdx)
		}
	}

	if localNode.RightHash != peerNode.RightHash {
		if localNode.RightHash != "" && peerNode.RightHash != "" {
			s.reconcileMismatchedNode(ctx, peerAddr, rightIdx)
		} else if peerNode.RightHash != "" {
			s.handlePotentialLeafMismatch(ctx, peerAddr, rightIdx)
		}
	}
}

// handlePotentialLeafMismatch decides whether to reconcile a leaf bucket or recurse.
func (s *antiEntropyService) handlePotentialLeafMismatch(ctx context.Context, peerAddr string, idx int32) {
	nodes, err := s.core.replication.GetMerkleNodes(ctx, peerAddr, []int32{idx})
	if err != nil || len(nodes) == 0 {
		return
	}

	node := nodes[0]
	if node.LeftHash == "" && node.RightHash == "" {
		if bucketID, ok := s.leafBucketID(idx); ok {
			s.reconcileBucket(ctx, peerAddr, bucketID)
		}
		return
	}

	s.reconcileMismatchedNode(ctx, peerAddr, idx)
}

// leafBucketID maps a Merkle leaf index to bucket ID.
func (s *antiEntropyService) leafBucketID(idx int32) (int32, bool) {
	bucketID := idx - merkleLeafOffset
	if bucketID < 0 {
		return 0, false
	}
	return bucketID, true
}

// reconcileBucket compares items in one bucket and repairs missing/mismatched chunks.
func (s *antiEntropyService) reconcileBucket(ctx context.Context, peerAddr string, bucketID int32) {
	logger.Infow("AE: Reconciling bucket", "bucket", bucketID, "peer", peerAddr)

	peerItems, err := s.core.replication.ListItemsInBucket(ctx, peerAddr, bucketID)
	if err != nil {
		return
	}

	for _, item := range peerItems {
		exists, _ := s.core.storage.HasChunk(ctx, domain.ChunkID(item.ChunkID))
		if !exists {
			if !s.isLocalReplicaOwner(item.ChunkID) {
				continue
			}

			logger.Infow("AE: Missing chunk found, pulling", "chunkID", item.ChunkID, "peer", peerAddr)
			if err := s.core.syncer.syncChunk(ctx, item.ChunkID, peerAddr); err != nil {
				logger.Warnw("AE: Failed to pull chunk", "chunkID", item.ChunkID, "error", err)
			}
			continue
		}

		if item.Checksum == 0 {
			continue
		}

		localChecksum, reader, err := s.core.storage.ReadChunk(ctx, domain.ChunkID(item.ChunkID))
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, reader)
		_ = reader.Close()

		if localChecksum == item.Checksum {
			continue
		}

		logger.Infow("AE: Checksum mismatch detected, pulling fresh chunk", "chunkID", item.ChunkID, "peer", peerAddr)
		if err := s.core.syncer.syncChunk(ctx, item.ChunkID, peerAddr); err != nil {
			logger.Warnw("AE: Failed to repair mismatched chunk", "chunkID", item.ChunkID, "error", err)
		}
	}
}

// isLocalReplicaOwner returns true when this node is in runtime replicas for chunk.
func (s *antiEntropyService) isLocalReplicaOwner(chunkID string) bool {
	token := murmur3.Sum64([]byte(chunkID))
	strategy := shard.NewShardStrategy(s.core.ring)
	replicas := strategy.GetReplicas(token, 0)
	for _, replica := range replicas {
		if replica.ID == s.core.nodeID {
			return true
		}
	}
	return false
}
