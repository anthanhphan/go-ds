package shard

import (
	"sort"
)

// PlacementStrategy defines how replicas are selected.
type PlacementStrategy interface {
	GetReplicas(token uint64, rf int) []Node
}

// SimpleStrategy places replicas on the next N distinct physical nodes on the ring.
// It is topology-unaware (doesn't know about racks/zones).
type SimpleStrategy struct {
	ring *Ring
}

func NewSimpleStrategy(ring *Ring) *SimpleStrategy {
	return &SimpleStrategy{ring: ring}
}

// GetReplicas finds 'rf' distinct physical nodes responsible for the given token.
func (s *SimpleStrategy) GetReplicas(token uint64, rf int) []Node {
	s.ring.mu.RLock()
	defer s.ring.mu.RUnlock()

	if len(s.ring.nodes) == 0 {
		return nil
	}

	// If requested replicas >= total nodes, return all nodes
	if rf >= len(s.ring.nodes) {
		nodes := make([]Node, 0, len(s.ring.nodes))
		for _, n := range s.ring.nodes {
			nodes = append(nodes, n)
		}
		return nodes
	}

	replicas := make([]Node, 0, rf)
	seen := make(map[string]bool)

	// Binary search start index
	idx := sort.Search(len(s.ring.vnodes), func(i int) bool {
		return s.ring.vnodes[i].Token >= token
	})
	if idx == len(s.ring.vnodes) {
		idx = 0
	}

	// Walk the ring clockwise
	for len(replicas) < rf {
		vnode := s.ring.vnodes[idx]
		if !seen[vnode.NodeID] {
			replicas = append(replicas, s.ring.nodes[vnode.NodeID])
			seen[vnode.NodeID] = true
		}

		idx = (idx + 1) % len(s.ring.vnodes)

		// Safety break if we've cycled the whole ring and still don't have enough (shouldn't happen due to check above)
		if idx == len(s.ring.vnodes) && len(seen) == len(s.ring.nodes) {
			break
		}
	}

	return replicas
}
