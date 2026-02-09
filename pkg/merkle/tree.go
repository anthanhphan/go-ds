package merkle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// MerkleTree implements a fixed-size heap-based Merkle Tree.
// The tree is stored as a flattened array where:
// - Index 0 is the root.
// - Left Child of i: 2i + 1
// - Right Child of i: 2i + 2
// - Parent of i: (i-1) / 2
type MerkleTree struct {
	mu         sync.RWMutex
	nodes      []string // Flattened array of hashes (hex strings)
	numLeaves  int      // Must be a power of 2
	treeSize   int      // Total number of nodes in the full binary tree
	leafOffset int      // Index in nodes where leaves start
}

// NewMerkleTree creates a new Merkle Tree with the specified number of leaves.
// numLeaves must be a power of 2 (e.g., 1024).
func NewMerkleTree(numLeaves int) (*MerkleTree, error) {
	if numLeaves < 2 || (numLeaves&(numLeaves-1)) != 0 {
		return nil, fmt.Errorf("numLeaves must be a power of 2 and >= 2")
	}

	// For a perfect binary tree, total nodes = 2*numLeaves - 1
	treeSize := 2*numLeaves - 1
	nodes := make([]string, treeSize)

	// Initialize empty hashes (can be empty string or hash of 0)
	// We'll use empty strings to represent "not set" or "empty bucket"
	// But the root will always be computed.

	return &MerkleTree{
		nodes:      nodes,
		numLeaves:  numLeaves,
		treeSize:   treeSize,
		leafOffset: numLeaves - 1,
	}, nil
}

// UpdateBucket updates the hash of a specific leaf (bucket) and propagates it to the root.
func (t *MerkleTree) UpdateBucket(bucketID int, leafHash string) error {
	if bucketID < 0 || bucketID >= t.numLeaves {
		return fmt.Errorf("bucketID out of range: %d", bucketID)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	idx := t.leafOffset + bucketID
	t.nodes[idx] = leafHash

	// Propagate up to root
	for idx > 0 {
		parentIdx := (idx - 1) / 2
		var left, right string

		if parentIdx*2+1 < t.treeSize {
			left = t.nodes[parentIdx*2+1]
		}
		if parentIdx*2+2 < t.treeSize {
			right = t.nodes[parentIdx*2+2]
		}

		t.nodes[parentIdx] = t.hashPair(left, right)
		idx = parentIdx
	}

	return nil
}

// GetRoot returns the current root hash of the tree.
func (t *MerkleTree) GetRoot() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nodes[0]
}

// GetNode returns the hash at a specific index.
func (t *MerkleTree) GetNode(idx int) (string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if idx < 0 || idx >= t.treeSize {
		return "", fmt.Errorf("index out of range")
	}
	return t.nodes[idx], nil
}

// GetChildren returns the hashes of the children of the node at idx.
func (t *MerkleTree) GetChildren(idx int) (string, string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	leftIdx := 2*idx + 1
	rightIdx := 2*idx + 2

	if leftIdx >= t.treeSize {
		return "", "", fmt.Errorf("node at %d is a leaf", idx)
	}

	return t.nodes[leftIdx], t.nodes[rightIdx], nil
}

// hashPair hashes two child hashes together.
func (t *MerkleTree) hashPair(left, right string) string {
	// If both are empty, we can return empty or a deterministic hash of "nothing"
	if left == "" && right == "" {
		return ""
	}

	h := sha256.New()
	h.Write([]byte(left))
	h.Write([]byte(right))
	return hex.EncodeToString(h.Sum(nil))
}

// NumLeaves returns the capacity of the tree.
func (t *MerkleTree) NumLeaves() int {
	return t.numLeaves
}

// ExportState returns the entire nodes array for persistence.
func (t *MerkleTree) ExportState() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	state := make([]string, len(t.nodes))
	copy(state, t.nodes)
	return state
}

// ImportState restores the tree from a saved nodes array.
func (t *MerkleTree) ImportState(nodes []string) error {
	if len(nodes) != t.treeSize {
		return fmt.Errorf("state size mismatch: expected %d, got %d", t.treeSize, len(nodes))
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	copy(t.nodes, nodes)
	return nil
}
