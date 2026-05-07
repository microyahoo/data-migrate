package common

import (
	"time"
)

type TaskRequest struct {
	ClientID string `json:"client_id"`
	Ready    bool   `json:"ready"`
}

type TaskResult struct {
	SourceDir string        `json:"source_dir"`
	TaskID    int           `json:"task_id"`
	Success   bool          `json:"success"`
	Message   string        `json:"message"`
	ClientID  string        `json:"client_id"`
	Duration  time.Duration `json:"duration"`

	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Objects   int64     `json:"objects"`
	Size      string    `json:"size"`
	Bytes     int64     `json:"bytes"`
}
