package domain

import (
	"errors"
	"hash/crc32"
)

const (
	// MaxChunkSize is the maximum size of a chunk in bytes (e.g., 16MB).
	// This should match the system-wide configuration.
	MaxChunkSize = 16 * 1024 * 1024
)

var (
	ErrChunkTooLarge   = errors.New("chunk exceeds maximum size")
	ErrInvalidChecksum = errors.New("invalid chunk checksum")
)

// ChunkID is the unique identifier for a chunk.
// It is derived from hash(file_id + chunk_index).
type ChunkID string

// Chunk represents a fixed-size block of data.
type Chunk struct {
	ID   ChunkID
	Data []byte
	// Checksum is the CRC32 checksum of the Data.
	Checksum uint32
}

// NewChunk creates a new Chunk and validates it.
func NewChunk(id ChunkID, data []byte) (*Chunk, error) {
	if len(data) > MaxChunkSize {
		return nil, ErrChunkTooLarge
	}

	checksum := crc32.ChecksumIEEE(data)

	return &Chunk{
		ID:       id,
		Data:     data,
		Checksum: checksum,
	}, nil
}

// Validate checks if the chunk data matches its checksum.
func (c *Chunk) Validate() error {
	if len(c.Data) > MaxChunkSize {
		return ErrChunkTooLarge
	}

	currentChecksum := crc32.ChecksumIEEE(c.Data)
	if currentChecksum != c.Checksum {
		return ErrInvalidChecksum
	}

	return nil
}
