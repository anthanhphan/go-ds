package service

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/anthanhphan/go-distributed-file-storage/internal/storage/domain"
)

// buildChunkID deterministically maps file ID and chunk index to a chunk key.
func buildChunkID(fileID string, chunkIndex int) domain.ChunkID {
	raw := fmt.Sprintf("%s-%d", fileID, chunkIndex)
	h := sha256.Sum256([]byte(raw))
	return domain.ChunkID(hex.EncodeToString(h[:]))
}

// buildMetadataID returns the metadata key used for a file.
func buildMetadataID(fileID string) string {
	return "meta:" + fileID
}
