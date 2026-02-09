package shard

import (
	"fmt"
)

// Node represents a physical node in the cluster.
type Node struct {
	ID        string     `json:"id"`
	Addr      string     `json:"addr"`
	ShardID   string     `json:"shard_id"`
	ReplicaID int        `json:"replica_id"`
	Status    NodeStatus `json:"status"`
}

type NodeStatus string

const (
	NodeStatusHealthy   NodeStatus = "healthy"
	NodeStatusUnhealthy NodeStatus = "unhealthy"
	NodeStatusLeft      NodeStatus = "left"
)

func (n Node) String() string {
	return fmt.Sprintf("%s@%s[%s]", n.ID, n.Addr, n.Status)
}

// VNode represents a virtual node on the ring.
// It points to a physical Node.
type VNode struct {
	Token  uint64
	NodeID string
}

// MerkleNode represents a node in the Merkle Tree for RPC transport.
type MerkleNode struct {
	Index     int32
	Hash      string
	LeftHash  string
	RightHash string
}

// BucketItem represents a single data item within a Merkle bucket.
type BucketItem struct {
	ChunkID  string
	Checksum uint32
}
