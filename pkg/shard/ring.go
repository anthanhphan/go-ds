package shard

import (
	"fmt"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

const (
	// DefaultVNodesPerNode is the default number of virtual nodes per physical node.
	// A higher number improves distribution balance but increases ring size.
	DefaultVNodesPerNode = 256
)

// Ring manages the consistent hashing ring.
type Ring struct {
	mu            sync.RWMutex
	vnodes        []VNode // Sorted list of all vnodes on the ring
	nodes         map[string]Node
	vnodesPerNode int
}

// NewRing creates a new consistent hashing ring.
func NewRing(vnodesPerNode int) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = DefaultVNodesPerNode
	}
	return &Ring{
		vnodes:        make([]VNode, 0),
		nodes:         make(map[string]Node),
		vnodesPerNode: vnodesPerNode,
	}
}

// AddNode adds a physical node to the ring.
func (r *Ring) AddNode(node Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if node.Status == "" {
		node.Status = NodeStatusHealthy
	}

	if existing, exists := r.nodes[node.ID]; exists {
		// Keep vnode ownership stable by node ID while allowing metadata refresh
		// (e.g. IP/port changes after restart).
		if existing.Addr != node.Addr || existing.Status != node.Status {
			r.nodes[node.ID] = node
		}
		return
	}

	r.nodes[node.ID] = node

	// Generate vnodes for this node
	for i := 0; i < r.vnodesPerNode; i++ {
		token := r.hashKey(fmt.Sprintf("%s-%d", node.ID, i))
		r.vnodes = append(r.vnodes, VNode{
			Token:  token,
			NodeID: node.ID,
		})
	}

	// Sort vnodes by token
	sort.Slice(r.vnodes, func(i, j int) bool {
		return r.vnodes[i].Token < r.vnodes[j].Token
	})
}

// SetNodeStatus updates the likelihood of a node without removing its vnodes.
func (r *Ring) SetNodeStatus(nodeID string, status NodeStatus) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if node, exists := r.nodes[nodeID]; exists {
		node.Status = status
		r.nodes[nodeID] = node
	}
}

// RemoveNode removes a physical node from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.nodes[nodeID]; !exists {
		return
	}

	delete(r.nodes, nodeID)

	// Filter out vnodes belonging to this node
	newVNodes := make([]VNode, 0, len(r.vnodes))
	for _, vn := range r.vnodes {
		if vn.NodeID != nodeID {
			newVNodes = append(newVNodes, vn)
		}
	}
	r.vnodes = newVNodes
}

// LocateKey finds the primary node that owns the given key.
func (r *Ring) LocateKey(key []byte) Node {
	token := r.hashData(key)
	return r.LocateToken(token)
}

// LocateToken finds the primary node that owns the given token.
func (r *Ring) LocateToken(token uint64) Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.vnodes) == 0 {
		return Node{}
	}

	// Binary search for the first vnode with token >= target token
	idx := sort.Search(len(r.vnodes), func(i int) bool {
		return r.vnodes[i].Token >= token
	})

	// Wrap around to the first vnode if we reached the end
	if idx == len(r.vnodes) {
		idx = 0
	}

	nodeID := r.vnodes[idx].NodeID
	return r.nodes[nodeID]
}

// GetNodes returns all physical nodes in the ring.
func (r *Ring) GetNodes() []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	nodes := make([]Node, 0, len(r.nodes))
	for _, n := range r.nodes {
		nodes = append(nodes, n)
	}
	return nodes
}

// GetShardNodes returns all nodes belonging to a specific shard.
func (r *Ring) GetShardNodes(shardID string) []Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var nodes []Node
	for _, n := range r.nodes {
		if n.ShardID == shardID {
			nodes = append(nodes, n)
		}
	}
	return nodes
}

// GetShards returns a list of all unique shard IDs in the ring.
func (r *Ring) GetShards() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	shardsMap := make(map[string]struct{})
	for _, n := range r.nodes {
		if n.ShardID != "" {
			shardsMap[n.ShardID] = struct{}{}
		}
	}

	shards := make([]string, 0, len(shardsMap))
	for s := range shardsMap {
		shards = append(shards, s)
	}
	sort.Strings(shards)
	return shards
}

// hashKey generates a token for a string key.
// Using Murmur3 for better distribution as per requirements.
func (r *Ring) hashKey(key string) uint64 {
	return r.hashData([]byte(key))
}

func (r *Ring) hashData(data []byte) uint64 {
	return murmur3.Sum64(data)
}
