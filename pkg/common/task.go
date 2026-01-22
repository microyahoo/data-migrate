package common

import (
	"time"
)

type TaskRequest struct {
	ClientID string `json:"client_id"`
	Ready    bool   `json:"ready"`
}

type TaskResult struct {
	SourceDir    string        `json:"source_dir"`
	TaskID       int           `json:"task_id"`
	Success      bool          `json:"success"`
	Message      string        `json:"message"`
	ClientID     string        `json:"client_id"`
	SplitPattern string        `json:"split_pattern"`
	SplitFiles   int           `json:"split_files"`
	LogFile      string        `json:"log_file"`
	Duration     time.Duration `json:"duration"`

	SubTaskID int       `json:"sub_task_id"`
	FileFrom  string    `json:"file_from"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	Objects   int64     `json:"objects"`
	Size      string    `json:"size"`
	Bytes     int64     `json:"bytes"`
}
