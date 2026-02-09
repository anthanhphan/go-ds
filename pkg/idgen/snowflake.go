package idgen

import (
	"errors"
	"sync"
)

const (
	// Configuration for 64-bit ID:
	// 1 bit: Unused (sign bit)
	// 41 bits: Timestamp (milliseconds) - gives ~69 years
	// 10 bits: Node ID - gives 1024 nodes
	// 12 bits: Sequence - gives 4096 IDs per millisecond per node

	nodeBits     = 10
	sequenceBits = 12

	maxNodeID   = -1 ^ (-1 << nodeBits)
	maxSequence = -1 ^ (-1 << sequenceBits)

	nodeShift      = sequenceBits
	timestampShift = sequenceBits + nodeBits

	// Custom Epoch (e.g., 2024-01-01 00:00:00 UTC)
	Epoch = 1704067200000
)

var (
	ErrNodeIDTooLarge = errors.New("node ID too large")
	ErrClockMovedBack = errors.New("clock moved backwards")
)

// Snowflake generates unique 64-bit IDs.
type Snowflake struct {
	mu       sync.Mutex
	clock    Clock
	nodeID   int64
	lastTime int64
	sequence int64
}

// New creates a new Snowflake ID generator.
func New(nodeID int64, clock Clock) (*Snowflake, error) {
	if nodeID < 0 || nodeID > int64(maxNodeID) {
		return nil, ErrNodeIDTooLarge
	}

	if clock == nil {
		clock = &SystemClock{}
	}

	return &Snowflake{
		clock:    clock,
		nodeID:   nodeID,
		lastTime: -1,
		sequence: 0,
	}, nil
}

// Next generates the next unique ID.
func (s *Snowflake) Next() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.clock.Now()

	if now < s.lastTime {
		return 0, ErrClockMovedBack
	}

	if now == s.lastTime {
		s.sequence = (s.sequence + 1) & int64(maxSequence)
		if s.sequence == 0 {
			// Sequence exhausted, wait for next millisecond
			for now <= s.lastTime {
				now = s.clock.Now()
			}
		}
	} else {
		s.sequence = 0
	}

	s.lastTime = now

	id := ((now - Epoch) << timestampShift) |
		(s.nodeID << nodeShift) |
		(s.sequence)

	return id, nil
}
