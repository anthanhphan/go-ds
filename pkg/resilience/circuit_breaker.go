package resilience

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

// CircuitOpenError reports circuit-open status with a concrete retry delay.
type CircuitOpenError struct {
	Name       string
	RetryAfter time.Duration
}

func (e *CircuitOpenError) Error() string {
	retryAfter := e.RetryAfter
	if retryAfter < 0 {
		retryAfter = 0
	}
	if e.Name == "" {
		return fmt.Sprintf("%v: retry in %s", ErrCircuitOpen, retryAfter)
	}
	return fmt.Sprintf("%v for %s: retry in %s", ErrCircuitOpen, e.Name, retryAfter)
}

func (e *CircuitOpenError) Is(target error) bool {
	return target == ErrCircuitOpen
}

type CircuitBreakerState string

const (
	CircuitClosed   CircuitBreakerState = "closed"
	CircuitOpen     CircuitBreakerState = "open"
	CircuitHalfOpen CircuitBreakerState = "half_open"
)

type CircuitBreakerConfig struct {
	Name              string
	FailureThreshold  int
	SuccessThreshold  int
	OpenTimeout       time.Duration
	HalfOpenMaxFlight int
}

type CircuitBreaker struct {
	mu sync.Mutex

	cfg CircuitBreakerConfig

	state        CircuitBreakerState
	failureCount int
	successCount int
	openUntil    time.Time
	halfInFlight int
}

func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.SuccessThreshold <= 0 {
		cfg.SuccessThreshold = 1
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 10 * time.Second
	}
	if cfg.HalfOpenMaxFlight <= 0 {
		cfg.HalfOpenMaxFlight = 1
	}

	return &CircuitBreaker{
		cfg:   cfg,
		state: CircuitClosed,
	}
}

func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.refreshStateLocked(time.Now())
	return cb.state
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	err := fn(ctx)

	// Do not penalize user-driven cancellation.
	if errors.Is(err, context.Canceled) {
		cb.afterCanceled()
		return err
	}

	if err != nil {
		cb.afterFailure()
		return err
	}

	cb.afterSuccess()
	return nil
}

func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.refreshStateLocked(now)

	switch cb.state {
	case CircuitOpen:
		return cb.openErrLocked(now)
	case CircuitHalfOpen:
		if cb.halfInFlight >= cb.cfg.HalfOpenMaxFlight {
			return cb.openErrLocked(now)
		}
		cb.halfInFlight++
		return nil
	default:
		return nil
	}
}

func (cb *CircuitBreaker) afterSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitHalfOpen:
		if cb.halfInFlight > 0 {
			cb.halfInFlight--
		}
		cb.successCount++
		if cb.successCount >= cb.cfg.SuccessThreshold {
			cb.toClosedLocked()
		}
	default:
		cb.failureCount = 0
	}
}

func (cb *CircuitBreaker) afterFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitHalfOpen:
		if cb.halfInFlight > 0 {
			cb.halfInFlight--
		}
		cb.toOpenLocked()
	default:
		cb.failureCount++
		if cb.failureCount >= cb.cfg.FailureThreshold {
			cb.toOpenLocked()
		}
	}
}

func (cb *CircuitBreaker) afterCanceled() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitHalfOpen && cb.halfInFlight > 0 {
		cb.halfInFlight--
	}
}

func (cb *CircuitBreaker) refreshStateLocked(now time.Time) {
	if cb.state == CircuitOpen && !now.Before(cb.openUntil) {
		cb.state = CircuitHalfOpen
		cb.failureCount = 0
		cb.successCount = 0
		cb.halfInFlight = 0
	}
}

func (cb *CircuitBreaker) toOpenLocked() {
	cb.state = CircuitOpen
	cb.openUntil = time.Now().Add(cb.cfg.OpenTimeout)
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfInFlight = 0
}

func (cb *CircuitBreaker) toClosedLocked() {
	cb.state = CircuitClosed
	cb.failureCount = 0
	cb.successCount = 0
	cb.halfInFlight = 0
}

func (cb *CircuitBreaker) openErrLocked(now time.Time) error {
	remaining := cb.openUntil.Sub(now)
	if remaining < 0 {
		remaining = 0
	}
	return &CircuitOpenError{
		Name:       cb.cfg.Name,
		RetryAfter: remaining,
	}
}
