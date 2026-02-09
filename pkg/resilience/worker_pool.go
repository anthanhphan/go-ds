package resilience

import (
	"context"
	"errors"
	"sync"
)

var ErrWorkerPoolClosed = errors.New("worker pool is closed")

type WorkerPool struct {
	jobs   chan func()
	closed bool
	mu     sync.RWMutex
	once   sync.Once
	wg     sync.WaitGroup
}

func NewWorkerPool(workers, queueSize int) *WorkerPool {
	if workers <= 0 {
		workers = 1
	}
	if queueSize <= 0 {
		queueSize = workers
	}

	p := &WorkerPool{
		jobs: make(chan func(), queueSize),
	}

	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for job := range p.jobs {
				if job != nil {
					job()
				}
			}
		}()
	}

	return p
}

func (p *WorkerPool) Submit(ctx context.Context, job func()) error {
	if job == nil {
		return nil
	}

	p.mu.RLock()
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		return ErrWorkerPoolClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		return nil
	}
}

func (p *WorkerPool) Close() {
	p.once.Do(func() {
		p.mu.Lock()
		p.closed = true
		close(p.jobs)
		p.mu.Unlock()
	})
}

func (p *WorkerPool) Wait() {
	p.wg.Wait()
}
