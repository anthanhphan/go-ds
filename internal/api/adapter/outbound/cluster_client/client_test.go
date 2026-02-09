package cluster_client

import (
	"context"
	"testing"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"google.golang.org/grpc"
)

// Mock Client
type MockStorageClient struct {
	storagev1.StorageServiceClient // Embed interface
	GetClusterTopologyFunc         func(ctx context.Context, in *storagev1.GetClusterTopologyRequest, opts ...grpc.CallOption) (*storagev1.GetClusterTopologyResponse, error)
}

func (m *MockStorageClient) GetClusterTopology(ctx context.Context, in *storagev1.GetClusterTopologyRequest, opts ...grpc.CallOption) (*storagev1.GetClusterTopologyResponse, error) {
	if m.GetClusterTopologyFunc != nil {
		return m.GetClusterTopologyFunc(ctx, in, opts...)
	}
	return nil, nil
}

func TestClusterClient_PollTopology(t *testing.T) {
	ring := shard.NewRing(10)
	seeds := []string{"seed1:8080"}
	client := NewClusterClient(ring, seeds, 100*time.Millisecond)

	mockClient := &MockStorageClient{}
	client.SetClientFactory(func(addr string) (storagev1.StorageServiceClient, error) {
		return mockClient, nil
	})

	// Setup expectation
	expectedTopology := &storagev1.GetClusterTopologyResponse{
		Nodes: []*storagev1.GetClusterTopologyResponse_Node{
			{Id: "node1", Addr: "node1:8080", ShardId: "shard1", ReplicaId: 0},
			{Id: "node2", Addr: "node2:8080", ShardId: "shard2", ReplicaId: 0},
		},
	}
	mockClient.GetClusterTopologyFunc = func(ctx context.Context, in *storagev1.GetClusterTopologyRequest, opts ...grpc.CallOption) (*storagev1.GetClusterTopologyResponse, error) {
		return expectedTopology, nil
	}

	// Run poll
	ctx := context.Background()
	client.PollTopology(ctx)

	nodes := ring.GetNodes()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}

	// Verify one node
	found := false
	for _, n := range nodes {
		if n.ID == "node1" {
			found = true
			if n.Addr != "node1:8080" {
				t.Errorf("expected node1 addr to be node1:8080, got %s", n.Addr)
			}
			break
		}
	}
	if !found {
		t.Error("expected node1 to be in ring")
	}
}

func TestClusterClient_PollTopology_WithFailure(t *testing.T) {
	ring := shard.NewRing(10)
	seeds := []string{"seed1:8080"}
	client := NewClusterClient(ring, seeds, 100*time.Millisecond)

	mockClient := &MockStorageClient{}
	client.SetClientFactory(func(addr string) (storagev1.StorageServiceClient, error) {
		return mockClient, nil
	})

	// Setup expectation: failure
	mockClient.GetClusterTopologyFunc = func(ctx context.Context, in *storagev1.GetClusterTopologyRequest, opts ...grpc.CallOption) (*storagev1.GetClusterTopologyResponse, error) {
		return nil, context.DeadlineExceeded
	}

	// Run poll
	ctx := context.Background()
	client.PollTopology(ctx)

	nodes := ring.GetNodes()
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes after failure, got %d", len(nodes))
	}
}

func TestClusterClient_UpdateRing_IgnoresUnusableAddr(t *testing.T) {
	ring := shard.NewRing(10)
	ring.AddNode(shard.Node{
		ID:        "node1",
		Addr:      "172.18.0.4:8081",
		ShardID:   "shard1",
		ReplicaID: 1,
	})

	client := NewClusterClient(ring, nil, 100*time.Millisecond)
	client.updateRing([]*storagev1.GetClusterTopologyResponse_Node{
		{
			Id:        "node1",
			Addr:      "0.0.0.0:8081",
			ShardId:   "shard1",
			ReplicaId: 1,
		},
	})

	nodes := ring.GetNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Addr != "172.18.0.4:8081" {
		t.Fatalf("expected addr to be preserved, got %s", nodes[0].Addr)
	}
}

func TestIsUsableNodeAddr(t *testing.T) {
	cases := []struct {
		addr   string
		usable bool
	}{
		{addr: "", usable: false},
		{addr: "0.0.0.0:8081", usable: false},
		{addr: "[::]:8081", usable: false},
		{addr: "172.18.0.4:8081", usable: true},
		{addr: "storage-1:8081", usable: true},
	}

	for _, tc := range cases {
		if got := isUsableNodeAddr(tc.addr); got != tc.usable {
			t.Fatalf("isUsableNodeAddr(%q) = %v, want %v", tc.addr, got, tc.usable)
		}
	}
}
