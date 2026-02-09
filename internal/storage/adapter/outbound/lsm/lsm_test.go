package lsm

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
)

func TestLSMAdapter_WriteRead(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()

	chunk, _ := domain.NewChunk("chunk1", []byte("hello world"))

	ctx := context.Background()
	if err := adapter.WriteChunk(ctx, chunk.ID, "", chunk.Checksum, bytes.NewReader(chunk.Data)); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	_, reader, err := adapter.ReadChunk(ctx, "chunk1")
	if err != nil {
		t.Fatalf("ReadChunk failed: %v", err)
	}
	defer func() { _ = reader.Close() }()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}

	if string(data) != "hello world" {
		t.Errorf("Expected 'hello world', got '%s'", string(data))
	}
}

func TestLSMAdapter_Persistence(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_persist_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Write in first session
	adapter1, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	chunk1, _ := domain.NewChunk("c1", []byte("data1"))
	chunk2, _ := domain.NewChunk("c2", []byte("data2"))

	ctx := context.Background()
	_ = adapter1.WriteChunk(ctx, chunk1.ID, "", chunk1.Checksum, bytes.NewReader(chunk1.Data))
	_ = adapter1.WriteChunk(ctx, chunk2.ID, "", chunk2.Checksum, bytes.NewReader(chunk2.Data))
	_ = adapter1.Close()

	// Open in second session
	adapter2, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter2.Close() }()

	// Check index
	has1, _ := adapter2.HasChunk(ctx, "c1")
	if !has1 {
		t.Errorf("Chunk c1 lost")
	}

	_, reader2, err := adapter2.ReadChunk(ctx, "c2")
	if err != nil {
		t.Errorf("Failed to read c2: %v", err)
	} else {
		defer func() { _ = reader2.Close() }()
		data2, _ := io.ReadAll(reader2)
		if string(data2) != "data2" {
			t.Errorf("Corrupt data for c2")
		}
	}
}

func TestLSMAdapter_NotFound(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_404")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, _ := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	defer func() { _ = adapter.Close() }()

	_, _, err = adapter.ReadChunk(context.Background(), "missing")
	if err != port.ErrChunkNotFound {
		t.Errorf("Expected ErrChunkNotFound, got %v", err)
	}
}

func TestLSMAdapter_Persistence_Index(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_index_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// 1. Write data
	adapter1, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	chunk1, _ := domain.NewChunk("c1", []byte("data1"))
	_ = adapter1.WriteChunk(context.Background(), chunk1.ID, "", 0, bytes.NewReader(chunk1.Data))

	// Close should save index
	_ = adapter1.Close()

	// 2. Open again - should load from index (fast)
	adapter2, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter2.Close() }()

	has, _ := adapter2.HasChunk(context.Background(), "c1")
	if !has {
		t.Errorf("Chunk c1 not found after reload with index")
	}
}

func BenchmarkWriteChunk(b *testing.B) {
	dir, err := os.MkdirTemp("", "lsm_bench")
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir, FSync: false}) // Disable fsync for bench
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()

	ctx := context.Background()
	data := make([]byte, 1024)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			chunkID := domain.ChunkID(fmt.Sprintf("bench-%d-%d", b.N, i))
			_ = adapter.WriteChunk(ctx, chunkID, "", 0, bytes.NewReader(data))
			i++
		}
	})
}

func TestLSMAdapter_DeleteChunk(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lsm_delete_test")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	cfg := config.LSMConfig{
		DataDir: tmpDir,
		FSync:   false,
	}

	adapter, err := NewLSMAdapter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()

	ctx := context.Background()
	chunkID := domain.ChunkID("chunk-to-delete")
	data := []byte("hello world")
	checksum := 0
	reader := bytes.NewReader(data)

	// Write
	if err := adapter.WriteChunk(ctx, chunkID, "", uint32(checksum), reader); err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	// Verify exists
	exists, err := adapter.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk failed: %v", err)
	}
	if !exists {
		t.Fatal("Chunk should exist")
	}

	// Delete
	if err := adapter.DeleteChunk(ctx, chunkID); err != nil {
		t.Fatalf("DeleteChunk failed: %v", err)
	}

	// Verify deleted
	exists, err = adapter.HasChunk(ctx, chunkID)
	if err != nil {
		t.Fatalf("HasChunk failed: %v", err)
	}
	if exists {
		t.Fatal("Chunk should not exist after deletion")
	}

	// Verify read returns not found
	_, _, err = adapter.ReadChunk(ctx, chunkID)
	if err != port.ErrChunkNotFound {
		t.Fatalf("ReadChunk should return ErrChunkNotFound, got: %v", err)
	}
}

func TestLSMAdapter_ReplayIgnoresStaleCheckpoint(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_stale_index")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	chunk, _ := domain.NewChunk("stale-checkpoint", []byte("fresh-data"))
	if err := adapter.WriteChunk(ctx, chunk.ID, "file-1", chunk.Checksum, bytes.NewReader(chunk.Data)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Simulate a valid but stale checkpoint index file.
	indexFile, err := os.Create(filepath.Join(dir, IndexFileName))
	if err != nil {
		t.Fatal(err)
	}
	enc := gob.NewEncoder(indexFile)
	if err := enc.Encode(uint64(1)); err != nil {
		t.Fatal(err)
	}
	if err := enc.Encode(map[domain.ChunkID]IndexEntry{}); err != nil {
		t.Fatal(err)
	}
	_ = indexFile.Close()

	adapter2, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter2.Close() }()

	exists, err := adapter2.HasChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("has chunk failed: %v", err)
	}
	if !exists {
		t.Fatalf("chunk should be recovered from segment replay")
	}
}

func TestLSMAdapter_ReplayTruncatesPartialTail(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_partial_tail")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	chunk, _ := domain.NewChunk("tail-check", []byte("tail-data"))
	if err := adapter.WriteChunk(ctx, chunk.ID, "file-1", chunk.Checksum, bytes.NewReader(chunk.Data)); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	segmentPath := filepath.Join(dir, fmt.Sprintf("%s%05d%s", SegmentPrefix, 1, SegmentSuffix))
	originalStat, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}

	// Append an incomplete record tail to simulate abrupt crash during write.
	f, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0, 0, 0, 10, 'b', 'a', 'd'}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	corruptedStat, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if corruptedStat.Size() <= originalStat.Size() {
		t.Fatalf("expected corrupted file to be larger")
	}

	adapter2, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer func() { _ = adapter2.Close() }()

	repairedStat, err := os.Stat(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if repairedStat.Size() != originalStat.Size() {
		t.Fatalf("expected partial tail to be truncated to %d, got %d", originalStat.Size(), repairedStat.Size())
	}

	_, reader, err := adapter2.ReadChunk(ctx, chunk.ID)
	if err != nil {
		t.Fatalf("read after repair failed: %v", err)
	}
	defer func() { _ = reader.Close() }()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read all failed: %v", err)
	}
	if string(data) != "tail-data" {
		t.Fatalf("unexpected data after repair: %s", string(data))
	}
}

func TestLSMAdapter_CompactPreservesWritesDuringCompaction(t *testing.T) {
	dir, err := os.MkdirTemp("", "lsm_compact_concurrent")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	adapter, err := NewLSMAdapter(config.LSMConfig{DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()

	adapter.maxSegmentSize = 8 * 1024 // force frequent segments
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), 2048)

	for i := 0; i < 600; i++ {
		id := domain.ChunkID(fmt.Sprintf("base-%d", i))
		if err := adapter.WriteChunk(ctx, id, "file-base", 0, bytes.NewReader(payload)); err != nil {
			t.Fatalf("preload write failed: %v", err)
		}
	}

	done := make(chan error, 1)
	go func() {
		done <- adapter.Compact()
	}()

	var lateWrites []domain.ChunkID
	for i := 0; i < 300; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("compaction failed: %v", err)
			}
			done = nil
		default:
		}

		id := domain.ChunkID(fmt.Sprintf("late-%d", i))
		if err := adapter.WriteChunk(ctx, id, "file-late", 0, bytes.NewReader(payload)); err != nil {
			t.Fatalf("late write failed: %v", err)
		}
		lateWrites = append(lateWrites, id)

		if done == nil {
			break
		}
	}

	if done != nil {
		if err := <-done; err != nil {
			t.Fatalf("compaction failed: %v", err)
		}
	}

	if len(lateWrites) == 0 {
		t.Fatalf("expected at least one write while compaction in progress")
	}

	for _, id := range lateWrites {
		exists, err := adapter.HasChunk(ctx, id)
		if err != nil {
			t.Fatalf("has chunk failed: %v", err)
		}
		if !exists {
			t.Fatalf("late write lost after compaction: %s", id)
		}
	}
}
