package grpc_handler

import (
	"context"
	"errors"
	"io"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNormalizeRPCErr(t *testing.T) {
	t.Run("grpc canceled to context canceled", func(t *testing.T) {
		err := normalizeRPCErr(context.Background(), status.Error(codes.Canceled, "canceled"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("eof with canceled context to context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := normalizeRPCErr(ctx, io.EOF)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	})

	t.Run("unavailable stays failure", func(t *testing.T) {
		input := status.Error(codes.Unavailable, "unavailable")
		err := normalizeRPCErr(context.Background(), input)
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("expected unavailable, got %v", err)
		}
	})
}
