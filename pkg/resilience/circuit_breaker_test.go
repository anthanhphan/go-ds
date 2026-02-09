package resilience

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitBreakerOpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 2,
		OpenTimeout:      200 * time.Millisecond,
	})

	fail := func(context.Context) error { return errors.New("boom") }

	if err := cb.Execute(context.Background(), fail); err == nil {
		t.Fatalf("expected first failure")
	}
	if err := cb.Execute(context.Background(), fail); err == nil {
		t.Fatalf("expected second failure")
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected circuit open, got %s", cb.State())
	}
	if err := cb.Execute(context.Background(), fail); !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected open error, got %v", err)
	}
}

func TestCircuitBreakerHalfOpenClosesOnSuccess(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "test",
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      100 * time.Millisecond,
	})

	_ = cb.Execute(context.Background(), func(context.Context) error {
		return errors.New("boom")
	})
	time.Sleep(120 * time.Millisecond)

	if err := cb.Execute(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatalf("expected success in half-open, got %v", err)
	}
	if cb.State() != CircuitClosed {
		t.Fatalf("expected circuit closed, got %s", cb.State())
	}
}

func TestCircuitBreakerOpenErrorCarriesRetryAfter(t *testing.T) {
	cb := NewCircuitBreaker(CircuitBreakerConfig{
		Name:             "node-a:8081",
		FailureThreshold: 1,
		OpenTimeout:      200 * time.Millisecond,
	})

	_ = cb.Execute(context.Background(), func(context.Context) error {
		return errors.New("boom")
	})

	err := cb.Execute(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	var openErr *CircuitOpenError
	if !errors.As(err, &openErr) {
		t.Fatalf("expected CircuitOpenError, got %T", err)
	}
	if openErr.RetryAfter <= 0 {
		t.Fatalf("expected positive retry_after, got %s", openErr.RetryAfter)
	}
	if openErr.Name != "node-a:8081" {
		t.Fatalf("expected name node-a:8081, got %s", openErr.Name)
	}
}
