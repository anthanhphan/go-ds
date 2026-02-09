package idgen

import (
	"testing"
	"time"
)

// MockClock for deterministic testing
type MockClock struct {
	CurrentTime int64
}

func (m *MockClock) Now() int64 {
	return m.CurrentTime
}

func TestSnowflake_Next(t *testing.T) {
	clock := &MockClock{CurrentTime: Epoch + 1000}
	nodeID := int64(1)
	sf, err := New(nodeID, clock)
	if err != nil {
		t.Fatalf("Failed to create Snowflake: %v", err)
	}

	id1, err := sf.Next()
	if err != nil {
		t.Fatalf("Failed to generate ID: %v", err)
	}

	id2, err := sf.Next()
	if err != nil {
		t.Fatalf("Failed to generate ID: %v", err)
	}

	if id1 == id2 {
		t.Errorf("IDs must be unique")
	}

	if id1 >= id2 {
		t.Errorf("IDs must be monotonic increasing")
	}
}

func TestSnowflake_NodeIDTooLarge(t *testing.T) {
	_, err := New(1024, nil) // max is 1023
	if err != ErrNodeIDTooLarge {
		t.Errorf("Expected ErrNodeIDTooLarge, got %v", err)
	}
}

func TestSnowflake_ClockMovedBack(t *testing.T) {
	clock := &MockClock{CurrentTime: Epoch + 2000}
	sf, _ := New(1, clock)

	_, _ = sf.Next()

	clock.CurrentTime = Epoch + 1000 // Move clock back
	_, err := sf.Next()

	if err != ErrClockMovedBack {
		t.Errorf("Expected ErrClockMovedBack, got %v", err)
	}
}

func TestSnowflake_Concurrency(t *testing.T) {
	sf, _ := New(1, &SystemClock{})
	numGoroutines := 50
	numIDs := 1000
	ids := make(chan int64, numGoroutines*numIDs)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			for j := 0; j < numIDs; j++ {
				id, err := sf.Next()
				if err != nil {
					t.Errorf("Concurrent generation failed: %v", err)
				}
				ids <- id
			}
		}()
	}

	uniqueMap := make(map[int64]bool)
	expectedCount := numGoroutines * numIDs
	for i := 0; i < expectedCount; i++ {
		select {
		case id := <-ids:
			if uniqueMap[id] {
				t.Errorf("Duplicate ID generated: %d", id)
			}
			uniqueMap[id] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for IDs")
		}
	}
}
