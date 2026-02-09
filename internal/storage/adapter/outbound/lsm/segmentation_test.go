package lsm

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
)

func TestLSMAdapter_Segmentation(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_segment_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Set a small max segment size to trigger rotation quickly
	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	adapter.maxSegmentSize = 100 // 100 bytes
	defer func() { _ = adapter.Close() }()

	ctx := context.Background()

	// Write chunks to trigger at least 3 segments
	for i := 0; i < 10; i++ {
		chunkID := domain.ChunkID(fmt.Sprintf("chunk-%d", i))
		data := []byte(fmt.Sprintf("data-chunk-content-%d", i)) // ~20 bytes + overhead
		chunk, _ := domain.NewChunk(chunkID, data)
		if err := adapter.WriteChunk(ctx, chunk.ID, "", chunk.Checksum, bytes.NewReader(chunk.Data)); err != nil {
			t.Fatalf("Failed to write chunk %d: %v", i, err)
		}
	}

	// Verify multiple segments exist
	matches, _ := os.ReadDir(dir)
	segmentCount := 0
	for _, entry := range matches {
		if !entry.IsDir() {
			segmentCount++
		}
	}
	if segmentCount < 2 {
		t.Errorf("Expected at least 2 segments, got %d", segmentCount)
	}

	// Verify all chunks can be read
	for i := 0; i < 10; i++ {
		chunkID := domain.ChunkID(fmt.Sprintf("chunk-%d", i))
		_, reader, err := adapter.ReadChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to read chunk %d: %v", i, err)
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			t.Fatalf("Failed to read chunk data %d: %v", i, err)
		}
		expectedData := fmt.Sprintf("data-chunk-content-%d", i)
		if string(data) != expectedData {
			t.Errorf("Data mismatch for chunk %d: expected %s, got %s", i, expectedData, string(data))
		}
	}

	// Verify persistence across restart
	_ = adapter.Close()
	adapter2, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter2.Close() }()

	for i := 0; i < 10; i++ {
		chunkID := domain.ChunkID(fmt.Sprintf("chunk-%d", i))
		_, reader, err := adapter2.ReadChunk(ctx, chunkID)
		if err != nil {
			t.Fatalf("Failed to read chunk %d after restart: %v", i, err)
		}
		_ = reader.Close()
	}
}
