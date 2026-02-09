package port

import (
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

// MembershipPort defines the interface for cluster membership and failure detection.
type MembershipPort interface {
	// Join joins an existing cluster using a list of seed nodes.
	Join(seeds []string) error

	// Leave gracefully leaves the cluster.
	Leave() error

	// Members returns the list of current healthy members.
	Members() []shard.Node

	// LocalNode returns the local node information.
	LocalNode() shard.Node
}
