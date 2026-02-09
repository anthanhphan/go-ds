package gossip

import (
	"encoding/json"
	"testing"
)

func TestDecodeMeta(t *testing.T) {
	meta := map[string]interface{}{
		"shard_id":    "shard-1",
		"replica_id":  1,
		"server_port": 8081,
	}
	data, _ := json.Marshal(meta)

	shardID, replicaID, serverPort := decodeMeta(data)

	if shardID != "shard-1" {
		t.Errorf("expected shard-1, got %s", shardID)
	}
	if replicaID != 1 {
		t.Errorf("expected 1, got %d", replicaID)
	}
	if serverPort != 8081 {
		t.Errorf("expected 8081, got %d", serverPort)
	}
}

func TestGossipAdapter_NodeMeta(t *testing.T) {
	g := &GossipAdapter{
		shardID:    "shard-2",
		replicaID:  2,
		serverPort: 8082,
	}

	data := g.NodeMeta(0)
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}

	if m["server_port"].(float64) != 8082 {
		t.Errorf("expected 8082, got %v", m["server_port"])
	}
}
