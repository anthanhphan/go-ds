package gossip

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/hashicorp/memberlist"
)

// GossipAdapter implements port.MembershipPort using memberlist.
type GossipAdapter struct {
	list *memberlist.Memberlist
	conf *memberlist.Config
	ring *shard.Ring

	nodeID     string
	addr       string
	port       int
	serverPort int
	shardID    string
	replicaID  int
}

// Ensure GossipAdapter implements Memberlist Delegate
var _ memberlist.Delegate = (*GossipAdapter)(nil) // Add delegate

// NewGossipAdapter creates a new membership adapter.
func NewGossipAdapter(nodeID string, bindAddr string, bindPort int, serverPort int, ring *shard.Ring, shardID string, replicaID int) (*GossipAdapter, error) {
	config := memberlist.DefaultLANConfig()
	config.Name = nodeID
	config.BindAddr = bindAddr
	config.BindPort = bindPort
	config.AdvertisePort = bindPort

	// Disable logging for now
	config.LogOutput = io.Discard

	adapter := &GossipAdapter{
		conf:       config,
		ring:       ring,
		nodeID:     nodeID,
		addr:       bindAddr,
		port:       bindPort,
		serverPort: serverPort,
		shardID:    shardID,
		replicaID:  replicaID,
	}

	config.Events = adapter   // Handle join/leave events
	config.Delegate = adapter // Handle metadata exchange

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %w", err)
	}
	adapter.list = list

	// Self-register in the ring
	addr := adapter.serverHost()
	ring.AddNode(shard.Node{
		ID:        nodeID,
		Addr:      net.JoinHostPort(addr, strconv.Itoa(serverPort)),
		ShardID:   shardID,
		ReplicaID: replicaID,
	})

	return adapter, nil
}

// Join joins the cluster using seed nodes.
func (g *GossipAdapter) Join(seeds []string) error {
	if len(seeds) > 0 {
		_, err := g.list.Join(seeds)
		if err != nil {
			return fmt.Errorf("failed to join cluster: %w", err)
		}
	}
	return nil
}

// Leave leaves the cluster.
func (g *GossipAdapter) Leave() error {
	// gracefully leave
	if err := g.list.Leave(time.Second * 5); err != nil {
		return err
	}
	return g.list.Shutdown()
}

// NodeMeta returns the local node metadata.
func (g *GossipAdapter) NodeMeta(limit int) []byte {
	data, err := json.Marshal(map[string]interface{}{
		"shard_id":    g.shardID,
		"replica_id":  g.replicaID,
		"server_port": g.serverPort,
	})
	if err != nil {
		logger.Warnw("failed to marshal gossip node meta", "error", err.Error())
		return nil
	}
	return data
}

// NotifyMsg, GetBroadcasts, LocalState, MergeRemoteState are not used here but required by Delegate
func (g *GossipAdapter) NotifyMsg([]byte)                           {}
func (g *GossipAdapter) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (g *GossipAdapter) LocalState(join bool) []byte                { return nil }
func (g *GossipAdapter) MergeRemoteState(buf []byte, join bool)     {}

// Members returns the list of current members.
func (g *GossipAdapter) Members() []shard.Node {
	members := g.list.Members()
	nodes := make([]shard.Node, 0, len(members))
	for _, m := range members {
		shardID, replicaID, serverPort := decodeMeta(m.Meta)
		addr := m.Addr.String()
		if serverPort > 0 {
			addr = net.JoinHostPort(addr, strconv.Itoa(serverPort))
		} else {
			addr = net.JoinHostPort(addr, strconv.Itoa(int(m.Port)))
		}
		nodes = append(nodes, shard.Node{
			ID:        m.Name,
			Addr:      addr,
			ShardID:   shardID,
			ReplicaID: replicaID,
		})
	}
	return nodes
}

// LocalNode returns the local node info.
func (g *GossipAdapter) LocalNode() shard.Node {
	addr := g.serverHost()
	return shard.Node{
		ID:        g.nodeID,
		Addr:      net.JoinHostPort(addr, strconv.Itoa(g.serverPort)),
		ShardID:   g.shardID,
		ReplicaID: g.replicaID,
	}
}

// NotifyJoin is invoked when a node joins.
func (g *GossipAdapter) NotifyJoin(node *memberlist.Node) {
	shardID, replicaID, serverPort := decodeMeta(node.Meta)
	addr := node.Addr.String()
	if serverPort > 0 {
		addr = net.JoinHostPort(addr, strconv.Itoa(serverPort))
	} else {
		addr = net.JoinHostPort(addr, strconv.Itoa(int(node.Port)))
	}
	n := shard.Node{
		ID:        node.Name,
		Addr:      addr,
		ShardID:   shardID,
		ReplicaID: replicaID,
	}
	logger.Infow("Node joined", "id", n.ID, "shard", n.ShardID, "replica", n.ReplicaID, "addr", n.Addr)

	// Always add nodes to the ring (Gateway or Storage is no longer distinguished by Type)
	g.ring.AddNode(n)
}

// NotifyLeave is invoked when a node leaves.
func (g *GossipAdapter) NotifyLeave(node *memberlist.Node) {
	logger.Infow("Node left", "id", node.Name)
	g.ring.SetNodeStatus(node.Name, shard.NodeStatusUnhealthy)
}

// NotifyUpdate is invoked when a node is updated.
func (g *GossipAdapter) NotifyUpdate(node *memberlist.Node) {
	// Re-add to ring to update metadata
	g.NotifyJoin(node)
}

func decodeMeta(meta []byte) (string, int, int) {
	if len(meta) == 0 {
		return "", 0, 0
	}
	type nodeMeta struct {
		ShardID    string `json:"shard_id"`
		ReplicaID  int    `json:"replica_id"`
		ServerPort int    `json:"server_port"`
	}
	var m nodeMeta
	if err := json.Unmarshal(meta, &m); err != nil {
		logger.Warnw("failed to decode node metadata", "error", err.Error())
		return "", 0, 0
	}

	return m.ShardID, m.ReplicaID, m.ServerPort
}

func (g *GossipAdapter) serverHost() string {
	if g.addr == "" {
		return g.addr
	}
	if ip := net.ParseIP(g.addr); ip == nil || !ip.IsUnspecified() {
		return g.addr
	}

	if g.list == nil || g.list.LocalNode() == nil {
		return g.addr
	}

	adv := g.list.LocalNode().Addr.String()
	if adv == "" {
		return g.addr
	}
	if ip := net.ParseIP(adv); ip != nil && ip.IsUnspecified() {
		return g.addr
	}
	return adv
}
