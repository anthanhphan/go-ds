package cluster_client

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	// "github.com/anthanhphan/go-distributed-file-storage/internal/storage/port" // No, we are in API. API shouldn't depend on internal/storage/port?
	// The API app depends on shard.Ring.
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	storagev1 "github.com/anthanhphan/go-distributed-file-storage/proto/gen/storage/v1"
	"github.com/anthanhphan/gosdk/logger"
)

type ClusterClient struct {
	ring          *shard.Ring
	seeds         []string
	clients       map[string]storagev1.StorageServiceClient
	conns         map[string]*grpc.ClientConn
	targetBackoff map[string]time.Time
	targetFails   map[string]int
	mu            sync.RWMutex
	stop          chan struct{}
	pollInterval  time.Duration
	seedInterval  time.Duration
	lastSeedPoll  time.Time
	pollFailures  int
	clientFactory ClientFactory
}

type ClientFactory func(addr string) (storagev1.StorageServiceClient, error)

func NewClusterClient(ring *shard.Ring, seeds []string, pollInterval time.Duration) *ClusterClient {
	return &ClusterClient{
		ring:          ring,
		seeds:         seeds,
		clients:       make(map[string]storagev1.StorageServiceClient),
		conns:         make(map[string]*grpc.ClientConn),
		targetBackoff: make(map[string]time.Time),
		targetFails:   make(map[string]int),
		stop:          make(chan struct{}),
		pollInterval:  pollInterval,
		seedInterval:  time.Minute,
	}
}

func (c *ClusterClient) Start(ctx context.Context) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	// Initial poll
	c.PollTopology(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stop:
			return
		case <-ticker.C:
			c.PollTopology(ctx)
		}
	}
}

func (c *ClusterClient) Stop() {
	close(c.stop)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

func (c *ClusterClient) PollTopology(ctx context.Context) {
	now := time.Now()
	uniqueTargets := make(map[string]struct{})

	currentNodes := c.ring.GetNodes()
	for _, n := range currentNodes {
		if n.Addr != "" {
			uniqueTargets[n.Addr] = struct{}{}
		}
	}

	if len(currentNodes) == 0 || c.shouldPollSeeds(now) {
		for _, s := range c.seeds {
			uniqueTargets[s] = struct{}{}
		}
		c.markSeedPoll(now)
	}

	filteredTargets := make([]string, 0, len(uniqueTargets))
	for addr := range uniqueTargets {
		if c.shouldSkipTarget(addr, now) {
			continue
		}
		filteredTargets = append(filteredTargets, addr)
	}

	// If all candidates are in backoff window, still probe seeds as fallback.
	if len(filteredTargets) == 0 && len(c.seeds) > 0 {
		filteredTargets = append(filteredTargets, c.seeds...)
	}

	if len(filteredTargets) == 0 {
		return
	}

	type pollResult struct {
		addr  string
		nodes []*storagev1.GetClusterTopologyResponse_Node
	}

	resultChan := make(chan pollResult, len(filteredTargets))
	pollCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, addr := range filteredTargets {
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			nodes, err := c.getTopologyFrom(pollCtx, a)
			if err == nil {
				c.recordTargetSuccess(a)
				select {
				case resultChan <- pollResult{addr: a, nodes: nodes}:
					cancel() // Stop other polls on first success
				case <-pollCtx.Done():
				}
			} else {
				if isIgnorablePollError(err) {
					return
				}
				c.recordTargetFailure(a)
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				logger.Debugw("Failed to poll topology from node", "addr", a, "error", err.Error())
			}
		}(addr)
	}

	// Wait for first success or all failures
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case result := <-resultChan:
		c.recordPollSuccess()
		c.updateRing(result.nodes)
	case <-done:
		c.recordPollFailure()
		if firstErr != nil {
			logger.Warnw("Failed to poll topology from all known nodes", "error", firstErr.Error())
		} else {
			logger.Warnw("Failed to poll topology from all known nodes")
		}
	case <-ctx.Done():
	}
}

func (c *ClusterClient) getTopologyFrom(ctx context.Context, addr string) ([]*storagev1.GetClusterTopologyResponse_Node, error) {
	client, err := c.getClient(addr)
	if err != nil {
		return nil, err
	}

	// Short timeout for topology check
	tCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.GetClusterTopology(tCtx, &storagev1.GetClusterTopologyRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Nodes, nil
}

func (c *ClusterClient) getClient(addr string) (storagev1.StorageServiceClient, error) {
	c.mu.RLock()
	client, ok := c.clients[addr]
	c.mu.RUnlock()
	if ok {
		return client, nil
	}

	var (
		newClient storagev1.StorageServiceClient
		newConn   *grpc.ClientConn
		err       error
	)
	if c.clientFactory != nil {
		newClient, err = c.clientFactory(addr)
	} else {
		newConn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			newClient = storagev1.NewStorageServiceClient(newConn)
		}
	}
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if client, ok := c.clients[addr]; ok {
		if newConn != nil {
			_ = newConn.Close()
		}
		return client, nil
	}

	c.clients[addr] = newClient
	if newConn != nil {
		c.conns[addr] = newConn
	}
	return newClient, nil
}

func (c *ClusterClient) updateRing(pbNodes []*storagev1.GetClusterTopologyResponse_Node) {
	// Identify current nodes in ring
	currentNodes := c.ring.GetNodes()
	currentMap := make(map[string]shard.Node)
	for _, n := range currentNodes {
		currentMap[n.ID] = n
	}

	// Identify new nodes
	newMap := make(map[string]shard.Node)
	for _, pn := range pbNodes {
		if !isUsableNodeAddr(pn.Addr) {
			// Ignore wildcard/self-bind addresses from node reports to avoid poisoning ring routing.
			if existing, exists := currentMap[pn.Id]; exists {
				newMap[pn.Id] = existing
				logger.Debugw("Ignoring unusable topology addr, keeping current addr",
					"id", pn.Id, "reported_addr", pn.Addr, "current_addr", existing.Addr)
			} else {
				logger.Warnw("Ignoring unusable topology addr", "id", pn.Id, "reported_addr", pn.Addr)
			}
			continue
		}

		// Assume all returned nodes are storage nodes
		node := shard.Node{
			ID:        pn.Id,
			Addr:      pn.Addr,
			ShardID:   pn.ShardId,
			ReplicaID: int(pn.ReplicaId),
		}
		newMap[node.ID] = node

		// Add or update node if metadata changed.
		if existing, exists := currentMap[node.ID]; !exists {
			logger.Infow("Adding node to ring", "id", node.ID, "addr", node.Addr)
			c.ring.AddNode(node)
		} else if existing.Addr != node.Addr || existing.ShardID != node.ShardID || existing.ReplicaID != node.ReplicaID {
			logger.Infow("Updating node in ring", "id", node.ID, "addr", node.Addr)
			c.ring.RemoveNode(node.ID)
			c.ring.AddNode(node)
		}
	}

	// Remove stale nodes
	for id := range currentMap {
		if _, exists := newMap[id]; !exists {
			logger.Infow("Removing node from ring", "id", id)
			c.dropClientByAddr(currentMap[id].Addr)
			c.ring.RemoveNode(id)
		}
	}
}

// SetClientFactory sets the client factory for testing purposes
func (c *ClusterClient) SetClientFactory(f ClientFactory) {
	c.clientFactory = f
}

func isIgnorablePollError(err error) bool {
	if err == nil {
		return true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

func isUsableNodeAddr(addr string) bool {
	if strings.TrimSpace(addr) == "" {
		return false
	}

	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if host == "" {
		return false
	}

	ip := net.ParseIP(host)
	if ip != nil && ip.IsUnspecified() {
		return false
	}
	return true
}

func (c *ClusterClient) shouldPollSeeds(now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if now.Sub(c.lastSeedPoll) >= c.seedInterval {
		return true
	}
	return c.pollFailures >= 3
}

func (c *ClusterClient) markSeedPoll(ts time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastSeedPoll = ts
}

func (c *ClusterClient) shouldSkipTarget(addr string, now time.Time) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	next, ok := c.targetBackoff[addr]
	return ok && now.Before(next)
}

func (c *ClusterClient) recordTargetFailure(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.targetFails[addr]++
	failCount := c.targetFails[addr]
	if failCount > 6 {
		failCount = 6
	}
	backoff := c.pollInterval * time.Duration(1<<failCount)
	if backoff > time.Minute {
		backoff = time.Minute
	}
	c.targetBackoff[addr] = time.Now().Add(backoff)
	c.dropClientLocked(addr)
}

func (c *ClusterClient) recordTargetSuccess(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.targetFails, addr)
	delete(c.targetBackoff, addr)
}

func (c *ClusterClient) recordPollSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollFailures = 0
}

func (c *ClusterClient) recordPollFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pollFailures++
}

func (c *ClusterClient) dropClientByAddr(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dropClientLocked(addr)
}

func (c *ClusterClient) dropClientLocked(addr string) {
	if conn, ok := c.conns[addr]; ok {
		_ = conn.Close()
		delete(c.conns, addr)
	}
	delete(c.clients, addr)
}
