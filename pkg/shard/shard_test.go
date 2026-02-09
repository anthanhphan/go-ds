package shard

import (
	"testing"
)

func TestRing_AddRemoveNode(t *testing.T) {
	ring := NewRing(10)

	node1 := Node{ID: "node1", Addr: "127.0.0.1:8081"}
	ring.AddNode(node1)

	if len(ring.nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(ring.nodes))
	}
	if len(ring.vnodes) != 10 {
		t.Errorf("Expected 10 vnodes, got %d", len(ring.vnodes))
	}

	node2 := Node{ID: "node2", Addr: "127.0.0.1:8082"}
	ring.AddNode(node2)

	if len(ring.nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(ring.nodes))
	}
	if len(ring.vnodes) != 20 {
		t.Errorf("Expected 20 vnodes, got %d", len(ring.vnodes))
	}

	ring.RemoveNode("node1")
	if len(ring.nodes) != 1 {
		t.Errorf("Expected 1 node, got %d", len(ring.nodes))
	}
	if len(ring.vnodes) != 10 {
		t.Errorf("Expected 10 vnodes, got %d", len(ring.vnodes))
	}

	// Check that remaining vnodes are from node2
	for _, vn := range ring.vnodes {
		if vn.NodeID != "node2" {
			t.Errorf("Expected vnode to belong to node2, got %s", vn.NodeID)
		}
	}
}

func TestRing_LocateKey(t *testing.T) {
	ring := NewRing(10)
	node1 := Node{ID: "node1", Addr: "1"}
	node2 := Node{ID: "node2", Addr: "2"}
	ring.AddNode(node1)
	ring.AddNode(node2)

	// Just verification that it returns a valid node
	owner := ring.LocateKey([]byte("some-file-key"))
	if owner.ID != "node1" && owner.ID != "node2" {
		t.Errorf("LocateKey returned unknown node: %v", owner)
	}
}

func TestSimpleStrategy_GetReplicas(t *testing.T) {
	ring := NewRing(10)
	nodes := []Node{
		{ID: "n1", Addr: "1"},
		{ID: "n2", Addr: "2"},
		{ID: "n3", Addr: "3"},
		{ID: "n4", Addr: "4"},
		{ID: "n5", Addr: "5"},
	}

	for _, n := range nodes {
		ring.AddNode(n)
	}

	strategy := NewSimpleStrategy(ring)

	// Test RF=3
	token := uint64(12345) // Arbitrary token
	replicas := strategy.GetReplicas(token, 3)

	if len(replicas) != 3 {
		t.Errorf("Expected 3 replicas, got %d", len(replicas))
	}

	// Check distinctness
	seen := make(map[string]bool)
	for _, r := range replicas {
		if seen[r.ID] {
			t.Errorf("Duplicate replica returned: %s", r.ID)
		}
		seen[r.ID] = true
	}

	// Test RF > Total Nodes
	replicasAll := strategy.GetReplicas(token, 10)
	if len(replicasAll) != 5 {
		t.Errorf("Expected 5 replicas (max nodes), got %d", len(replicasAll))
	}
}
