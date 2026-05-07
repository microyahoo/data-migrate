package worker

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/microyahoo/data-migrate/pkg/common"
)

const (
	defaultConcurrency = 3
)

type Worker struct {
	serverAddr  string
	clientID    string
	concurrency int
}

func NewWorker(serverAddr string, concurrency int) *Worker {
	hostname, _ := os.Hostname()
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	return &Worker{
		serverAddr:  serverAddr,
		clientID:    fmt.Sprintf("%s-%d", hostname, os.Getpid()),
		concurrency: concurrency,
	}
}

func (w *Worker) Start() error {
	for {
		conn, err := net.Dial("tcp", w.serverAddr)
		if err != nil {
			log.Errorf("Failed to connect to server: %v, retrying...", err)
			time.Sleep(15 * time.Second)
			continue
		}
		defer conn.Close()

		log.Infof("Connected to server %s as client %s", w.serverAddr, w.clientID)

		decoder := json.NewDecoder(conn)
		encoder := json.NewEncoder(conn)

		for {
			req := common.TaskRequest{
				ClientID: w.clientID,
				Ready:    true,
			}
			// send ready notification to server
			if err := encoder.Encode(req); err != nil {
				log.Errorf("Error sending request to server: %v", err)
				break
			}

			// receive migration task
			var task common.MigrationTask
			if err := decoder.Decode(&task); err != nil {
				log.Errorf("Error receiving task from server: %v", err)
				break
			}

			// if no more tasks
			if task.ID == 0 {
				log.Info("No more tasks from server")
				return nil
			}

			log.Infof("Received task %d from server", task.ID)

			// handle task
			result := w.executeTask(&task)

			// send migration result to server
			if err := encoder.Encode(result); err != nil {
				log.Errorf("Error sending result to server: %v", err)
				break
			}

			log.Infof("Task %d completed: success: %v", task.ID, result.Success)
		}

		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) executeTask(task *common.MigrationTask) *common.TaskResult {
	result := &common.TaskResult{
		SourceDir: task.SourceDir,
		TaskID:    task.ID,
		ClientID:  w.clientID,
	}

	if err := task.Check(); err != nil {
		result.Success = false
		result.Message = err.Error()
		return result
	}

	log.Printf("Executing task %d: %s with file list %s",
		task.ID, task.SourceDir, task.FileListPath)

	stats, err := common.WalkDirAndCount(task.SourceDir, task.FileListPath, task.Concurrency)
	message := fmt.Sprintf("Capacity statistics of task %d successfully", task.ID)

	if err != nil {
		result.Success = false
		result.Message = err.Error()
	} else {
		result.Success = true
		result.Message = message
		result.Objects = stats.Objects
		result.Bytes = stats.Bytes
		result.Size = common.ByteSize(uint64(stats.Bytes))
	}

	return result
}
