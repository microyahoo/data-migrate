package common

import "time"

type TaskRequest struct {
	ClientID string `json:"client_id"`
	Ready    bool   `json:"ready"`
}

type TaskResult struct {
	SourceDir   string        `json:"source_dir"`
	TargetDir   string        `json:"target_dir"`
	TaskID      int           `json:"task_id"`
	Success     bool          `json:"success"`
	Message     string        `json:"message"`
	ClientID    string        `json:"client_id"`
	IncludeFile string        `json:"include_file"`
	LogFile     string        `json:"log_file"`
	Duration    time.Duration `json:"duration"`
}
