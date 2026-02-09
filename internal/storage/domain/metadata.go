package domain

import "time"

// FileMetadata stores information about a file in the system.
type FileMetadata struct {
	FileID    string    `json:"file_id"`
	FileName  string    `json:"file_name"`
	Extension string    `json:"extension"`
	Size      int64     `json:"size"`
	Chunks    int       `json:"chunks"`
	CreatedAt time.Time `json:"created_at"`
}
