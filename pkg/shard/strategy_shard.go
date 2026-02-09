package shard

import (
	"sort"
)

// ShardStrategy implements a placement strategy where a key is mapped to a shard,
// and all nodes in that shard receive a replica.
type ShardStrategy struct {
	ring *Ring
}

func NewShardStrategy(ring *Ring) *ShardStrategy {
	return &ShardStrategy{ring: ring}
}

// GetReplicas finds the shard responsible for the token and returns all its nodes.
// Note: rf is ignored if shard-based replication is used strictly,
// as the number of replicas is determined by nodes per shard.
func (s *ShardStrategy) GetReplicas(token uint64, rf int) []Node {
	s.ring.mu.RLock()
	vnodesCount := len(s.ring.vnodes)
	s.ring.mu.RUnlock()

	if vnodesCount == 0 {
		return nil
	}

	// 1. Locate the vnode/shard on the ring
	s.ring.mu.RLock()
	idx := sort.Search(len(s.ring.vnodes), func(i int) bool {
		return s.ring.vnodes[i].Token >= token
	})
	if idx == len(s.ring.vnodes) {
		idx = 0
	}
	nodeID := s.ring.vnodes[idx].NodeID
	primaryNode := s.ring.nodes[nodeID]
	s.ring.mu.RUnlock()

	// 2. Get all nodes in the same shard
	return s.ring.GetShardNodes(primaryNode.ShardID)
}

// Ensure ShardStrategy implements PlacementStrategy
var _ PlacementStrategy = (*ShardStrategy)(nil)
