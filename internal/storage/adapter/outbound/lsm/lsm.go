package lsm

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/config"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/port"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/merkle"
	"github.com/anthanhphan/go-distributed-file-storage/pkg/shard"
	"github.com/anthanhphan/gosdk/logger"
	"github.com/spaolacci/murmur3"
)

// IndexEntry stores the location of a chunk in a specific segment file.
type IndexEntry struct {
	SegmentID uint64
	Offset    int64
	Size      int64
	ParentID  string
	Checksum  uint32
}

// LSMAdapter implements port.StoragePort using segmented append-only logs and an in-memory index.
type LSMAdapter struct {
	indexMu             sync.RWMutex
	fileMu              sync.Mutex
	compactionMu        sync.Mutex
	dirPath             string
	activeFile          *os.File
	activeFileID        uint64
	maxSegmentSize      int64
	index               map[domain.ChunkID]IndexEntry
	pool                *sync.Pool
	fsync               bool
	merkleTree          *merkle.MerkleTree
	compactionThreshold int
}

const (
	// DefaultMaxSegmentSize is 64MB
	DefaultMaxSegmentSize = 64 * 1024 * 1024
	SegmentPrefix         = "segment_"
	SegmentSuffix         = ".log"
	IndexFileName         = "index.gob"
)

// NewLSMAdapter initializes the storage engine.
// It tries to load the index from disk, then replays logs if necessary.
func NewLSMAdapter(cfg config.LSMConfig) (*LSMAdapter, error) {
	if err := os.MkdirAll(cfg.DataDir, 0750); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	adapter := &LSMAdapter{
		dirPath:        filepath.Clean(cfg.DataDir),
		index:          make(map[domain.ChunkID]IndexEntry),
		maxSegmentSize: DefaultMaxSegmentSize,
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 8*1024*1024+1024)
			},
		},
		fsync:               cfg.FSync,
		compactionThreshold: cfg.CompactionThreshold,
	}

	// Initialize Merkle Tree (1024 buckets as per design)
	tree, err := merkle.NewMerkleTree(1024)
	if err != nil {
		return nil, fmt.Errorf("failed to init merkle tree: %w", err)
	}
	adapter.merkleTree = tree

	// Always replay logs on startup for correctness.
	// The on-disk index is only a best-effort checkpoint.
	if err := adapter.replayLogs(); err != nil {
		return nil, fmt.Errorf("failed to replay logs: %w", err)
	}

	return adapter, nil
}

func (a *LSMAdapter) getSegmentPath(id uint64) string {
	return filepath.Join(a.dirPath, fmt.Sprintf("%s%05d%s", SegmentPrefix, id, SegmentSuffix))
}

func (a *LSMAdapter) getIndexPath() string {
	return filepath.Join(a.dirPath, IndexFileName)
}

func (a *LSMAdapter) saveIndex() error {
	a.indexMu.RLock()
	defer a.indexMu.RUnlock()

	f, err := os.Create(a.getIndexPath())
	if err != nil {
		return err
	}
	// Restore previous handling
	defer func() { _ = f.Close() }()

	enc := gob.NewEncoder(f)

	// Encode activeFileID first
	if err := enc.Encode(a.activeFileID); err != nil {
		return err
	}

	// Encode index
	if err := enc.Encode(a.index); err != nil {
		return err
	}

	return nil
}

func (a *LSMAdapter) openActiveFile() error {
	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	return a.openActiveFileLocked()
}

func (a *LSMAdapter) openActiveFileLocked() error {
	filePath := a.getSegmentPath(a.activeFileID)
	// If activeFileID is 0 (fresh start via loadIndex? shouldn't happen if saved correctly)
	// If it's 0, make it 1
	if a.activeFileID == 0 {
		a.activeFileID = 1
		filePath = a.getSegmentPath(a.activeFileID)
	}

	// G304: filePath is constructed from internal data dir and ID
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600) // #nosec G304
	if err != nil {
		return err
	}
	// Seek to end to be ready for append
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return err
	}
	a.activeFile = file
	return nil
}

// replayLogs reads all segment files and rebuilds the index.
func (a *LSMAdapter) replayLogs() error {
	if a.activeFile != nil {
		_ = a.activeFile.Close()
		a.activeFile = nil
	}

	matches, err := filepath.Glob(filepath.Join(a.dirPath, SegmentPrefix+"*"+SegmentSuffix))
	if err != nil {
		return err
	}

	var segmentIDs []uint64
	for _, m := range matches {
		var id uint64
		_, err := fmt.Sscanf(filepath.Base(m), SegmentPrefix+"%d"+SegmentSuffix, &id)
		if err == nil {
			segmentIDs = append(segmentIDs, id)
		}
	}
	sort.Slice(segmentIDs, func(i, j int) bool { return segmentIDs[i] < segmentIDs[j] })

	a.indexMu.Lock()
	a.index = make(map[domain.ChunkID]IndexEntry) // Clear index
	a.indexMu.Unlock()

	if len(segmentIDs) == 0 {
		a.activeFileID = 1
	} else {
		for _, id := range segmentIDs {
			filePath := a.getSegmentPath(id)
			if err := a.replaySegment(id, filePath); err != nil {
				return err
			}
			a.activeFileID = id
		}
	}

	return a.openActiveFile()
}

func (a *LSMAdapter) replaySegment(id uint64, path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0600) // #nosec G304
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	reader := bufio.NewReader(file)
	offset := int64(0)
	truncated := false

	for {
		headerBuf := make([]byte, 4)
		_, err := io.ReadFull(reader, headerBuf)
		if err != nil {
			if err == io.EOF {
				break
			}
			if err == io.ErrUnexpectedEOF {
				truncated = true
				break
			}
			return fmt.Errorf("failed to read id len: %w", err)
		}
		idLen := int64(binary.BigEndian.Uint32(headerBuf))
		if idLen <= 0 || idLen > 1024*1024 {
			truncated = true
			break
		}

		idBuf := make([]byte, idLen)
		if _, err := io.ReadFull(reader, idBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncated = true
				break
			}
			return fmt.Errorf("failed to read id: %w", err)
		}
		chunkID := domain.ChunkID(idBuf)

		if _, err := io.ReadFull(reader, headerBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncated = true
				break
			}
			return fmt.Errorf("failed to read data len: %w", err)
		}
		dataLen := int64(binary.BigEndian.Uint32(headerBuf))
		if dataLen < 0 || dataLen > a.maxSegmentSize*4 {
			truncated = true
			break
		}

		// Check for potential overflow before summing
		if idLen > 1024*1024 || dataLen > a.maxSegmentSize*4 {
			truncated = true
			break
		}
		totalEntrySize := int64(4) + int64(idLen) + int64(4) + dataLen + int64(4)
		if _, err := io.CopyN(io.Discard, reader, dataLen); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncated = true
				break
			}
			return fmt.Errorf("failed to skip data: %w", err)
		}
		checksumBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, checksumBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				truncated = true
				break
			}
			return fmt.Errorf("failed to read checksum: %w", err)
		}
		checksum := binary.BigEndian.Uint32(checksumBuf)

		a.indexMu.Lock()
		a.index[chunkID] = IndexEntry{
			SegmentID: id,
			Offset:    offset,
			Size:      totalEntrySize,
			ParentID:  "", // Cannot recover parentID from old format
			Checksum:  checksum,
		}
		a.indexMu.Unlock()

		// Update Merkle Tree during replay
		a.updateMerkleTree(chunkID)

		offset += totalEntrySize
	}

	if truncated {
		if err := file.Truncate(offset); err != nil {
			return fmt.Errorf("failed to truncate partial segment %d: %w", id, err)
		}
		logger.Warnw("Truncated partial segment tail during replay", "segment_id", id, "valid_bytes", offset)
	}

	return nil
}

func (a *LSMAdapter) updateMerkleTree(id domain.ChunkID) {
	// 1. Calculate Bucket ID
	hash := murmur3.Sum64([]byte(id))
	// G115: NumLeaves returns int, safe to cast to uint64 since it's a count
	numLeaves := uint64(a.merkleTree.NumLeaves()) // #nosec G115
	if numLeaves == 0 {
		return // Should not happen if initialized correctly
	}
	// Safe cast: result of % numLeaves is bounded by numLeaves, which is int (1024)
	bucketID := int32(hash % numLeaves) // #nosec G115

	// 2. Fetch all item IDs in this bucket to recompute leaf hash
	items, err := a.ListItemsInBucket(context.Background(), bucketID)
	if err != nil {
		logger.Warnw("Merkle update skipped, failed to list bucket", "bucket_id", bucketID, "error", err.Error())
		return
	}

	// 3. Compute combined hash of all items in bucket
	h := sha256.New()
	for _, item := range items {
		h.Write([]byte(item.ChunkID))
		// We could also include checksum here for better entropy
		// binary.Write(h, binary.BigEndian, item.Checksum)
	}

	_ = a.merkleTree.UpdateBucket(int(bucketID), hex.EncodeToString(h.Sum(nil)))
}

// WriteChunk writes a chunk to the active log file from a stream.
func (a *LSMAdapter) WriteChunk(ctx context.Context, id domain.ChunkID, parentID string, checksum uint32, reader io.Reader) error {
	// Idempotency check with Read Lock
	a.indexMu.RLock()
	_, exists := a.index[id]
	a.indexMu.RUnlock()
	if exists {
		// Drain incoming stream so upstream streaming goroutine can exit cleanly.
		_, _ = io.Copy(io.Discard, reader)
		return nil
	}

	// Serialize writes to file
	a.fileMu.Lock()
	defer a.fileMu.Unlock()

	// Double check implementation of rotation might close activeFile
	if a.activeFile == nil {
		return fmt.Errorf("storage closed")
	}

	idBytes := []byte(id)
	if len(idBytes) > 1024*1024 {
		return fmt.Errorf("id too long")
	}
	idLen := uint32(len(idBytes)) // #nosec G115

	// Get current offset
	offset, err := a.activeFile.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Format: ID_Len (4) | ID (N) | Data_Len (4) | Data (M) | Checksum (4)

	// Write ID_Len and ID
	header := make([]byte, 4+len(idBytes)+4)
	binary.BigEndian.PutUint32(header[0:4], idLen)
	copy(header[4:4+len(idBytes)], idBytes)
	// Placeholder for Data_Len is at header[4+len(idBytes):]

	if _, err := a.activeFile.Write(header); err != nil {
		return err
	}

	dataStartOffset, _ := a.activeFile.Seek(0, io.SeekCurrent)

	// Copy data from reader to file
	written, err := io.Copy(a.activeFile, reader)
	if err != nil {
		return fmt.Errorf("failed to stream data to disk: %w", err)
	}

	// Write Checksum
	checksumBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(checksumBuf, checksum)
	if _, err := a.activeFile.Write(checksumBuf); err != nil {
		return err
	}

	// Fill Data_Len
	totalEntrySize := int64(4+len(idBytes)+4) + written + 4

	// Seek back to the placeholder
	if _, err := a.activeFile.Seek(dataStartOffset-4, io.SeekStart); err != nil {
		return err
	}

	lenBuf := make([]byte, 4)
	if written > int64(^uint32(0)) {
		return fmt.Errorf("chunk too large")
	}
	binary.BigEndian.PutUint32(lenBuf, uint32(written)) // #nosec G115
	if _, err := a.activeFile.Write(lenBuf); err != nil {
		return err
	}

	// Seek back to end
	_, _ = a.activeFile.Seek(0, io.SeekEnd)

	if a.fsync {
		_ = a.activeFile.Sync()
	}

	// Update index with Write Lock
	a.indexMu.Lock()
	a.index[id] = IndexEntry{
		SegmentID: a.activeFileID,
		Offset:    offset,
		Size:      totalEntrySize,
		ParentID:  parentID,
		Checksum:  checksum,
	}
	a.indexMu.Unlock()

	// Update Merkle Tree
	a.updateMerkleTree(id)

	// Check for segment rotation
	if info, err := a.activeFile.Stat(); err == nil && info.Size() > a.maxSegmentSize {
		_ = a.activeFile.Close()
		a.activeFileID++
		filePath := filepath.Clean(a.getSegmentPath(a.activeFileID))
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0600) // #nosec G304
		if err == nil {
			a.activeFile = file

			// Trigger compaction if threshold reached
			activeID := int(a.activeFileID) // #nosec G115
			if a.compactionThreshold > 0 && activeID > a.compactionThreshold {
				go func() {
					if err := a.Compact(); err != nil {
						logger.Warnw("Background compaction failed", "error", err.Error())
					}
				}()
			}
		}
	}

	return nil
}

// ReadChunk returns a stream for the requested chunk.
func (a *LSMAdapter) ReadChunk(ctx context.Context, id domain.ChunkID) (uint32, io.ReadCloser, error) {
	a.indexMu.RLock()
	entry, exists := a.index[id]
	a.indexMu.RUnlock()

	if !exists {
		return 0, nil, port.ErrChunkNotFound
	}

	path := filepath.Clean(a.getSegmentPath(entry.SegmentID))
	// Open a dedicated file handle for reading to allow concurrency
	// independent of writer's activeFile cursor
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return 0, nil, err
	}

	// Read header at entry.Offset
	headerBuf := make([]byte, 1024)
	n, err := f.ReadAt(headerBuf, entry.Offset)
	// Restore previous handling
	if err != nil && err != io.EOF {
		_ = f.Close()
		return 0, nil, err
	}
	headerBuf = headerBuf[:n]

	if len(headerBuf) < 4 {
		_ = f.Close()
		return 0, nil, fmt.Errorf("header too short")
	}

	idLen := binary.BigEndian.Uint32(headerBuf[0:4])
	dataLenOffset := 4 + idLen

	if int(dataLenOffset+4) > len(headerBuf) {
		_ = f.Close()
		return 0, nil, fmt.Errorf("header too short for data len")
	}

	dataLen := binary.BigEndian.Uint32(headerBuf[dataLenOffset : dataLenOffset+4])
	dataStart := entry.Offset + int64(dataLenOffset) + 4

	// Read checksum at the end
	checksumBuf := make([]byte, 4)
	if _, err := f.ReadAt(checksumBuf, dataStart+int64(dataLen)); err != nil {
		_ = f.Close()
		return 0, nil, err
	}
	checksum := binary.BigEndian.Uint32(checksumBuf)

	if _, err := f.Seek(dataStart, io.SeekStart); err != nil {
		_ = f.Close()
		return 0, nil, err
	}

	return checksum, &limitedReadCloser{
		R: io.LimitReader(f, int64(dataLen)),
		C: f,
	}, nil
}

func (a *LSMAdapter) readChunkByEntry(_ domain.ChunkID, entry IndexEntry) (uint32, []byte, error) {
	path := filepath.Clean(a.getSegmentPath(entry.SegmentID))
	f, err := os.Open(path) // #nosec G304
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = f.Close() }()

	headerBuf := make([]byte, 1024)
	n, err := f.ReadAt(headerBuf, entry.Offset)
	if err != nil && err != io.EOF {
		return 0, nil, err
	}
	headerBuf = headerBuf[:n]
	if len(headerBuf) < 4 {
		return 0, nil, fmt.Errorf("header too short")
	}

	idLen := binary.BigEndian.Uint32(headerBuf[0:4])
	dataLenOffset := 4 + idLen
	if int(dataLenOffset+4) > len(headerBuf) {
		return 0, nil, fmt.Errorf("header too short for data len")
	}

	dataLen := binary.BigEndian.Uint32(headerBuf[dataLenOffset : dataLenOffset+4])
	dataStart := entry.Offset + int64(dataLenOffset) + 4
	if int64(dataLen) > a.maxSegmentSize*4 {
		return 0, nil, fmt.Errorf("invalid data len")
	}

	data := make([]byte, int(dataLen))
	if _, err := f.ReadAt(data, dataStart); err != nil {
		return 0, nil, err
	}

	checksumBuf := make([]byte, 4)
	if _, err := f.ReadAt(checksumBuf, dataStart+int64(dataLen)); err != nil {
		return 0, nil, err
	}
	checksum := binary.BigEndian.Uint32(checksumBuf)

	return checksum, data, nil
}

type limitedReadCloser struct {
	R io.Reader
	C io.Closer
}

func (l *limitedReadCloser) Read(p []byte) (n int, err error) { return l.R.Read(p) }
func (l *limitedReadCloser) Close() error                     { return l.C.Close() }

// HasChunk checks if the chunk exists in the index.
func (a *LSMAdapter) HasChunk(ctx context.Context, id domain.ChunkID) (bool, error) {
	a.indexMu.RLock()
	defer a.indexMu.RUnlock()
	_, exists := a.index[id]
	return exists, nil
}

// DeleteChunk removes the chunk from the index.
// Note: Space is not reclaimed until compaction.
func (a *LSMAdapter) DeleteChunk(ctx context.Context, id domain.ChunkID) error {
	a.indexMu.Lock()
	deleted := false
	if _, exists := a.index[id]; exists {
		delete(a.index, id)
		deleted = true
	}
	a.indexMu.Unlock()

	if deleted {
		// Recompute Merkle bucket after deletion.
		a.updateMerkleTree(id)
	}
	return nil
}

// ListItemsInBucket returns all chunks mapping to a specific Merkle bucket.
func (a *LSMAdapter) ListItemsInBucket(ctx context.Context, bucketID int32) ([]shard.BucketItem, error) {
	a.indexMu.RLock()
	defer a.indexMu.RUnlock()

	var items []shard.BucketItem
	// G115: NumLeaves returns int, safe to cast to uint64
	numLeaves := uint64(a.merkleTree.NumLeaves()) // #nosec G115

	// This is a naive O(N) scan. In production, we'd want a bucket-to-item index.
	// But for a simple PoC, scanning the in-memory map is acceptable.
	for id := range a.index {
		if numLeaves == 0 {
			continue
		}
		hash := murmur3.Sum64([]byte(id))
		// Safe cast: result is bounded by numLeaves
		itemBucketID := int32(hash % numLeaves) // #nosec G115
		if bucketID == -1 || itemBucketID == bucketID {
			entry := a.index[id]
			items = append(items, shard.BucketItem{
				ChunkID:  string(id),
				Checksum: entry.Checksum,
			})
		}
	}

	// Sort items by ChunkID for deterministic leaf hashing
	sort.Slice(items, func(i, j int) bool {
		return items[i].ChunkID < items[j].ChunkID
	})

	return items, nil
}

func (a *LSMAdapter) GetMerkleRoot() string {
	return a.merkleTree.GetRoot()
}

func (a *LSMAdapter) GetMerkleNodes(indices []int32) ([]shard.MerkleNode, error) {
	var result []shard.MerkleNode
	for _, idx := range indices {
		h, err := a.merkleTree.GetNode(int(idx))
		if err != nil {
			continue
		}

		left, right, _ := a.merkleTree.GetChildren(int(idx))
		result = append(result, shard.MerkleNode{
			Index:     idx,
			Hash:      h,
			LeftHash:  left,
			RightHash: right,
		})
	}
	return result, nil
}

// Close closes the file handle and saves the index.
func (a *LSMAdapter) Close() error {
	// Save index first
	if err := a.saveIndex(); err != nil {
		logger.Warnw("Failed to save index on close", "error", err.Error())
	}

	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	if a.activeFile != nil {
		return a.activeFile.Close()
	}
	return nil
}

// ListAllKeys returns all chunk IDs currently in the index.
func (a *LSMAdapter) ListAllKeys() []domain.ChunkID {
	a.indexMu.RLock()
	defer a.indexMu.RUnlock()
	keys := make([]domain.ChunkID, 0, len(a.index))
	for k := range a.index {
		keys = append(keys, k)
	}
	return keys
}

// GetParentID returns the parent file ID for a given chunk.
func (a *LSMAdapter) GetParentID(id domain.ChunkID) (string, bool) {
	a.indexMu.RLock()
	defer a.indexMu.RUnlock()
	entry, exists := a.index[id]
	if !exists {
		return "", false
	}
	return entry.ParentID, true
}

// Compact merges segments and reclaims disk space by keeping only chunks that are in the current index.
func (a *LSMAdapter) Compact() error {
	a.compactionMu.Lock()
	defer a.compactionMu.Unlock()

	// Rotate active segment so new writes land outside the compaction snapshot.
	a.fileMu.Lock()
	if a.activeFile != nil {
		_ = a.activeFile.Sync()
		_ = a.activeFile.Close()
		a.activeFile = nil
	}
	oldActiveID := a.activeFileID
	a.activeFileID++
	if err := a.openActiveFileLocked(); err != nil {
		a.fileMu.Unlock()
		return fmt.Errorf("failed to open new active file during compaction: %w", err)
	}
	a.fileMu.Unlock()

	logger.Infow("Compaction started", "max_segment_id", oldActiveID)

	compactPath := filepath.Join(a.dirPath, "compact")
	if err := os.MkdirAll(compactPath, 0750); err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(compactPath) }()

	// Snapshot only segments that existed before rotation.
	a.indexMu.RLock()
	snapshot := make(map[domain.ChunkID]IndexEntry)
	for id, entry := range a.index {
		if entry.SegmentID <= oldActiveID {
			snapshot[id] = entry
		}
	}
	a.indexMu.RUnlock()

	keys := make([]domain.ChunkID, 0, len(snapshot))
	for id := range snapshot {
		keys = append(keys, id)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	newIndex := make(map[domain.ChunkID]IndexEntry, len(snapshot))

	curID := uint64(1)
	curPath := filepath.Clean(filepath.Join(compactPath, fmt.Sprintf("%s%05d%s", SegmentPrefix, curID, SegmentSuffix)))
	f, err := os.OpenFile(curPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600) // #nosec G304
	if err != nil {
		return err
	}

	offset := int64(0)
	for _, id := range keys {
		entry := snapshot[id]
		checksum, data, err := a.readChunkByEntry(id, entry)
		if err != nil {
			continue
		}

		idBytes := []byte(id)
		if len(idBytes) > 1024*1024 {
			continue // Should not happen for valid chunks
		}
		idLen := uint32(len(idBytes)) // #nosec G115
		header := make([]byte, 4+len(idBytes)+4)
		binary.BigEndian.PutUint32(header[0:4], idLen)
		copy(header[4:4+len(idBytes)], idBytes)
		if len(data) > int(a.maxSegmentSize*4) {
			continue
		}
		dataLen := uint32(len(data)) // #nosec G115
		binary.BigEndian.PutUint32(header[4+len(idBytes):], dataLen)

		if _, err := f.Write(header); err != nil {
			_ = f.Close()
			return err
		}
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return err
		}

		checksumBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(checksumBuf, checksum)
		if _, err := f.Write(checksumBuf); err != nil {
			_ = f.Close()
			return err
		}

		totalSize := int64(len(header) + len(data) + 4)
		newIndex[id] = IndexEntry{
			SegmentID: curID,
			Offset:    offset,
			Size:      totalSize,
			ParentID:  entry.ParentID,
			Checksum:  checksum,
		}
		offset += totalSize

		if offset > a.maxSegmentSize {
			if err := f.Close(); err != nil {
				return err
			}
			curID++
			curPath = filepath.Clean(filepath.Join(compactPath, fmt.Sprintf("%s%05d%s", SegmentPrefix, curID, SegmentSuffix)))
			f, err = os.OpenFile(curPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600) // #nosec G304
			if err != nil {
				return err
			}
			offset = 0
		}
	}

	if err := f.Close(); err != nil {
		return err
	}

	a.fileMu.Lock()
	a.indexMu.Lock()

	// Move compacted segments into permanent IDs above the active segment.
	matches, _ := filepath.Glob(filepath.Join(compactPath, SegmentPrefix+"*"+SegmentSuffix))
	sort.Strings(matches)

	nextSegmentID := a.activeFileID + 1
	for _, m := range matches {
		var oldTempID uint64
		if _, err := fmt.Sscanf(filepath.Base(m), SegmentPrefix+"%d"+SegmentSuffix, &oldTempID); err != nil {
			continue
		}
		destID := nextSegmentID
		nextSegmentID++
		dest := a.getSegmentPath(destID)
		if err := os.Rename(m, dest); err != nil {
			a.indexMu.Unlock()
			a.fileMu.Unlock()
			return err
		}

		for k, v := range newIndex {
			if v.SegmentID == oldTempID {
				v.SegmentID = destID
				newIndex[k] = v
			}
		}
	}

	// Preserve correctness under concurrent writes/deletes during compaction.
	for id := range newIndex {
		if _, exists := a.index[id]; !exists {
			delete(newIndex, id)
		}
	}
	for id, liveEntry := range a.index {
		if _, exists := newIndex[id]; !exists || liveEntry.SegmentID > oldActiveID {
			newIndex[id] = liveEntry
		}
	}
	a.index = newIndex

	referenced := make(map[uint64]struct{}, len(a.index)+1)
	for _, entry := range a.index {
		referenced[entry.SegmentID] = struct{}{}
	}
	referenced[a.activeFileID] = struct{}{}

	a.indexMu.Unlock()
	a.fileMu.Unlock()

	// Delete unreferenced segments after publishing new index.
	segmentFiles, _ := filepath.Glob(filepath.Join(a.dirPath, SegmentPrefix+"*"+SegmentSuffix))
	for _, segPath := range segmentFiles {
		var segID uint64
		if _, err := fmt.Sscanf(filepath.Base(segPath), SegmentPrefix+"%d"+SegmentSuffix, &segID); err != nil {
			continue
		}
		if _, keep := referenced[segID]; keep {
			continue
		}
		_ = os.Remove(segPath)
	}

	if err := a.saveIndex(); err != nil {
		return err
	}

	logger.Infow("Compaction finished", "compacted_segments_upto", oldActiveID, "live_keys", len(newIndex))
	return nil
}
