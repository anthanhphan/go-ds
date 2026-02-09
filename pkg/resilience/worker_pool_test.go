package resilience

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestWorkerPoolExecutesJobs(t *testing.T) {
	pool := NewWorkerPool(3, 6)
	defer pool.Close()

	var count int32
	for i := 0; i < 10; i++ {
		if err := pool.Submit(context.Background(), func() {
			atomic.AddInt32(&count, 1)
		}); err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	pool.Close()
	pool.Wait()

	if got := atomic.LoadInt32(&count); got != 10 {
		t.Fatalf("expected 10 jobs executed, got %d", got)
	}
}

func TestWorkerPoolSubmitAfterClose(t *testing.T) {
	pool := NewWorkerPool(1, 1)
	pool.Close()
	if err := pool.Submit(context.Background(), func() {}); err != ErrWorkerPoolClosed {
		t.Fatalf("expected ErrWorkerPoolClosed, got %v", err)
	}
}
