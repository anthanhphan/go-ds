package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/anthanhphan/go-distributed-file-storage/pkg/resilience"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
)

type replicaWriteFunc func(context.Context, shard.Node) error
type replicaErrFormatter func(shard.Node, error) error

// executeReplicaWrites runs replica writes in parallel and enforces ACK threshold.
func (s *FileServiceImpl) executeReplicaWrites(
	ctx context.Context,
	replicas []shard.Node,
	requiredSuccess int,
	writeFn replicaWriteFunc,
	formatErr replicaErrFormatter,
) (int, error) {
	total := len(replicas)
	resultChan := make(chan error, total)

	for _, replica := range replicas {
		go func(node shard.Node) {
			err := s.executeReplicaWriteWithRetry(ctx, node, writeFn)
			if err == nil {
				select {
				case resultChan <- nil:
				case <-ctx.Done():
				}
				return
			}

			select {
			case resultChan <- formatErr(node, err):
			case <-ctx.Done():
			}
		}(replica)
	}

	successCount := 0
	failCount := 0
	var lastErr error

	for i := 0; i < total; i++ {
		select {
		case err := <-resultChan:
			if err == nil {
				successCount++
				if successCount >= requiredSuccess {
					return successCount, nil
				}
				continue
			}

			failCount++
			lastErr = err
			if failCount > total-requiredSuccess {
				return successCount, lastErr
			}
		case <-ctx.Done():
			return successCount, ctx.Err()
		}
	}

	if successCount >= requiredSuccess {
		return successCount, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("replica writes failed without explicit error")
	}
	return successCount, lastErr
}

// executeReplicaWriteWithRetry retries a write attempt with timeout and backoff.
func (s *FileServiceImpl) executeReplicaWriteWithRetry(
	ctx context.Context,
	node shard.Node,
	writeFn replicaWriteFunc,
) error {
	// Bound total retry time per replica to keep request latency predictable.
	budgetCtx, cancelBudget := s.withWriteBudget(ctx)
	defer cancelBudget()

	maxRetries := s.maxRetries()
	if maxRetries <= 0 {
		maxRetries = 1
	}

	var lastErr error
	for retry := 0; retry < maxRetries; retry++ {
		attemptTimeout, ok := s.remainingAttemptTimeout(budgetCtx)
		if !ok {
			break
		}

		attemptCtx, cancel := context.WithTimeout(budgetCtx, attemptTimeout)
		err := writeFn(attemptCtx, node)
		cancel()
		if err == nil {
			return nil
		}

		lastErr = err
		if wait, isOpen := circuitOpenRetryAfter(lastErr); isOpen {
			if wait <= 0 {
				wait = time.Duration(retry+1) * 200 * time.Millisecond
			}
			if !sleepWithContext(budgetCtx, wait) {
				return budgetCtx.Err()
			}
			continue
		}

		select {
		case <-budgetCtx.Done():
			return budgetCtx.Err()
		case <-time.After(time.Duration(retry+1) * 100 * time.Millisecond):
		}
	}

	if lastErr == nil {
		if budgetCtx.Err() != nil {
			return budgetCtx.Err()
		}
		return fmt.Errorf("write failed without explicit error")
	}
	return lastErr
}

// withWriteBudget caps total write retry time to one configured write timeout window.
func (s *FileServiceImpl) withWriteBudget(ctx context.Context) (context.Context, context.CancelFunc) {
	budget := s.writeTimeout()
	targetDeadline := time.Now().Add(budget)

	if existingDeadline, ok := ctx.Deadline(); ok && existingDeadline.Before(targetDeadline) {
		return ctx, func() {}
	}
	return context.WithDeadline(ctx, targetDeadline)
}

// remainingAttemptTimeout returns per-attempt timeout capped by remaining budget.
func (s *FileServiceImpl) remainingAttemptTimeout(ctx context.Context) (time.Duration, bool) {
	base := s.writeTimeout()
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return 0, false
		}
		if remaining < base {
			return remaining, true
		}
	}
	return base, true
}

// circuitOpenRetryAfter extracts retry delay from circuit-open errors.
func circuitOpenRetryAfter(err error) (time.Duration, bool) {
	var openErr *resilience.CircuitOpenError
	if errors.As(err, &openErr) {
		return openErr.RetryAfter, true
	}
	if errors.Is(err, resilience.ErrCircuitOpen) {
		return 0, true
	}
	return 0, false
}

// sleepWithContext waits for delay or exits early if context is canceled.
func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
