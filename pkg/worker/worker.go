package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (w *Worker) uploadFile(f *os.File, s3Config *common.S3Configuration, key string) error {
	if s3Config == nil {
		return nil
	}
	bucket := s3Config.Bucket
	log.Infof("Start to upload results %s to s3 endpoint %s:%s", key, s3Config.Endpoint, bucket)
	ctx := context.Background()
	client, err := common.NewS3Client(ctx, s3Config.Endpoint, s3Config.AccessKey, s3Config.SecretKey,
		s3Config.Region, s3Config.SkipSSLVerify)
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		ACL:    types.ObjectCannedACLPublicRead,
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return err
	}
	return nil
}

func (w *Worker) executeTask(task *common.MigrationTask) *common.TaskResult {
	result := &common.TaskResult{
		SourceDir: task.SourceDir,
		TargetDir: task.TargetDir,
		TaskID:    task.ID,
		ClientID:  w.clientID,
		SubTaskID: task.SubTaskID,
		FileFrom:  task.FileFrom,
	}

	if err := task.Check(); err != nil {
		result.Success = false
		result.Message = err.Error()
		return result
	}

	log.Printf("Executing task %d: %s -> %s with file list %s or file from %s",
		task.ID, task.SourceDir, task.TargetDir, task.FileListPath, task.FileFrom)

	// Prepare rclone arguments
	rcloneFlags := task.RcloneFlags
	args := []string{"copy", task.SourceDir, task.TargetDir}
	if rcloneFlags.Checkers > 0 {
		args = append(args, "--checkers", fmt.Sprintf("%d", rcloneFlags.Checkers))
	}
	if rcloneFlags.Transfers > 0 {
		args = append(args, "--transfers", fmt.Sprintf("%d", rcloneFlags.Transfers))
	}
	if rcloneFlags.LocalNoSetModtime {
		args = append(args, "--local-no-set-modtime")
	}
	if rcloneFlags.SizeOnly {
		args = append(args, "--size-only")
	}
	if rcloneFlags.Dryrun {
		args = append(args, "--dry-run")
	}
	if rcloneFlags.LogLevel != "" {
		args = append(args, "--log-level", rcloneFlags.LogLevel)
	}

	var (
		logFiles     []string
		err          error
		splitPattern string
		splitFiles   int
		logFile      string
		message      string
	)

	if task.FileFrom != "" {
		logFile, err = w.processFile(0, task, args, task.FileFrom)
		message = fmt.Sprintf("Migrated task %d with subtask %d successfully", task.ID, task.SubTaskID)
	} else {
		splitPattern, splitFiles, err = w.executeWithFileList(task, args, &logFiles)
		message = fmt.Sprintf("Migrated task %d successfully", task.ID)
	}

	// Set result
	if len(logFiles) > 0 {
		logFile = logFiles[0] // simplify it
	}
	result.LogFile = logFile
	result.SplitPattern = splitPattern
	result.SplitFiles = splitFiles
	if err != nil {
		result.Success = false
		result.Message = err.Error()
	} else {
		result.Success = true
		result.Message = message
	}

	return result
}

// uploadLogFile uploads a log file to S3
func (w *Worker) uploadLogFile(logFile string, task *common.MigrationTask, s3Key string) error {
	f, err := os.Open(logFile)
	if err != nil {
		return fmt.Errorf("failed to open log file %s: %v", logFile, err)
	}
	defer f.Close()

	if err := w.uploadFile(f, task.S3Config, s3Key); err != nil {
		return fmt.Errorf("failed to upload log file %s to s3: %v", logFile, err)
	}

	return nil
}

// executeWithFileList executes rclone with file list using concurrent processing
// Returns: split pattern and file counts, and error
func (w *Worker) executeWithFileList(task *common.MigrationTask, baseArgs []string, logFiles *[]string) (string, int, error) {
	// Create directory for output files with "fileListDir/timestamp/client_id/task_id/"
	filesDir := filepath.Join(task.FileListDir, fmt.Sprintf("%d", task.Timestamp), w.clientID, fmt.Sprintf("%d", task.ID))
	if err := os.MkdirAll(filesDir, 0755); err != nil {
		return "", 0, fmt.Errorf("failed to create output directory: %v", err)
	}

	splitPattern := fmt.Sprintf("%s/%s_*.index", filesDir, filepath.Base(task.FileListPath))
	outputPrefix := filepath.Base(task.FileListPath)
	if task.FileListPath == "" {
		outputPrefix = filepath.Base(task.SourceDir)
		splitPattern = fmt.Sprintf("%s/%s_*.index", filesDir, outputPrefix)
	}
	// Get file channel from FindFiles
	fileChan, err := common.FindFiles(task.SourceDir, task.TargetDir, task.FileListPath,
		0,            /* list concurrency (let FindFiles decide) */
		filesDir,     /*output directory*/
		outputPrefix, /*output prefix*/
		task.MaxFilesPerOutput /*max files per output*/)
	if err != nil {
		return "", 0, fmt.Errorf("failed to find files: %v", err)
	}

	// Configurable parameters
	concurrency := min(w.concurrency, task.Concurrency) // Number of concurrent rclone processes
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}
	log.Infof("set concurrency to %d", concurrency)
	bufferSize := concurrency * 1000 // Buffer size for work channel

	var (
		// Create work channel with large buffer
		workChan = make(chan string, bufferSize)

		// Create error channel to collect errors from workers
		errChan = make(chan error, concurrency*10)

		// Track pending files and processed files
		pendingFiles   []string
		processedFiles []string
		mu             sync.Mutex

		// Track failed files
		failedFiles []string
	)

	// Create a file to save pending files
	pendingFile := fmt.Sprintf("/tmp/rclone_pending_%d_%d.txt", task.Timestamp, task.ID)
	pendingFileHandle, err := os.Create(pendingFile)
	if err != nil {
		return "", 0, fmt.Errorf("failed to create pending file: %v", err)
	}
	defer pendingFileHandle.Close()
	pendingWriter := bufio.NewWriter(pendingFileHandle)

	// Start workers
	var workerWg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		workerWg.Add(1)
		go func(workerID int) {
			defer workerWg.Done()
			w.rcloneWorker(workerID, task, baseArgs, workChan,
				&mu, &processedFiles, &failedFiles, logFiles, errChan)
		}(i)
	}

	// Start error collector
	errorWg := &sync.WaitGroup{}
	errorWg.Add(1)
	var collectedErrors []error

	go func() {
		defer errorWg.Done()
		for err := range errChan {
			collectedErrors = append(collectedErrors, err)
		}
	}()

	// Distribute files to workers
	distributeWg := &sync.WaitGroup{}
	distributeWg.Add(1)

	go func() {
		defer distributeWg.Done()
		fileCount := 0

		for file := range fileChan {
			fileCount++

			// Try to send to work channel
			select {
			case workChan <- file:
				// Successfully sent to worker
				log.Debugf("Distributed file %d: %s", fileCount, file)

			default:
				// Work channel is full (all workers busy)
				// Save to pending list and continue
				mu.Lock()
				pendingFiles = append(pendingFiles, file)
				// Also write to file for persistence
				pendingWriter.WriteString(file + "\n")
				mu.Unlock()

				log.Infof("Workers busy, saved file %d to pending list: %s", fileCount, file)
			}
		}

		// Flush pending file
		pendingWriter.Flush()

		log.Infof("Finished distributing %d files (%d pending)", fileCount, len(pendingFiles))
	}()

	// Wait for distribution to complete
	distributeWg.Wait()

	// Close work channel (all real-time files distributed)
	close(workChan)

	// Wait for all workers to complete current work
	workerWg.Wait()

	// Close error channel and wait for error collector
	close(errChan)
	errorWg.Wait()

	// Process pending files after workers finish
	if len(pendingFiles) > 0 {
		log.Infof("Processing %d pending files...", len(pendingFiles))

		// Create new work channel for pending files
		pendingWorkChan := make(chan string, concurrency)

		// Create new error channel for pending processing
		pendingErrChan := make(chan error, concurrency*10)

		// Start workers again for pending files
		var pendingWorkerWg sync.WaitGroup
		for i := 0; i < concurrency; i++ {
			pendingWorkerWg.Add(1)
			go func(workerID int) {
				defer pendingWorkerWg.Done()
				w.rcloneWorker(workerID+concurrency, task, baseArgs,
					pendingWorkChan, &mu, &processedFiles, &failedFiles,
					logFiles, pendingErrChan)
			}(i)
		}

		// Start pending error collector
		pendingErrorWg := &sync.WaitGroup{}
		pendingErrorWg.Add(1)
		var pendingCollectedErrors []error

		go func() {
			defer pendingErrorWg.Done()
			for err := range pendingErrChan {
				pendingCollectedErrors = append(pendingCollectedErrors, err)
			}
		}()

		// Send pending files to workers
		go func() {
			for _, file := range pendingFiles {
				pendingWorkChan <- file
			}
			close(pendingWorkChan)
		}()

		// Wait for pending files to be processed
		pendingWorkerWg.Wait()

		// Close pending error channel and collect errors
		close(pendingErrChan)
		pendingErrorWg.Wait()

		// Merge errors
		collectedErrors = append(collectedErrors, pendingCollectedErrors...)

		log.Infof("All pending files processed")
	} else {
		log.Info("No pending files to process")
	}

	// Final report
	totalFiles := len(processedFiles)
	totalFailed := len(failedFiles)
	log.Infof("Task completed: %d files processed, %d failed", totalFiles, totalFailed)

	// Clean up pending file
	os.Remove(pendingFile)

	// Return error if any files failed
	if len(collectedErrors) > 0 {
		if len(collectedErrors) == 1 {
			return splitPattern, totalFiles, fmt.Errorf("1 file failed: %v", collectedErrors[0])
		}

		// Create a summary error message
		errorSummary := fmt.Sprintf("%d files failed. First few errors:", len(collectedErrors))
		for i := 0; i < len(collectedErrors) && i < 3; i++ {
			errorSummary += fmt.Sprintf("\n- %v", collectedErrors[i])
		}
		if len(collectedErrors) > 3 {
			errorSummary += fmt.Sprintf("\n... and %d more errors", len(collectedErrors)-3)
		}

		// Also log all failed files
		if len(failedFiles) > 0 {
			log.Errorf("Failed files: %v", failedFiles)
		}

		return splitPattern, totalFiles, fmt.Errorf("%s", errorSummary)
	}

	return splitPattern, totalFiles, nil
}

// rcloneWorker processes rclone commands for files and returns errors via channel
func (w *Worker) rcloneWorker(workerID int, task *common.MigrationTask, baseArgs []string,
	workChan <-chan string, mu *sync.Mutex,
	processedFiles *[]string, failedFiles *[]string, logFiles *[]string, errChan chan<- error) {

	for file := range workChan {
		// Process the file
		s3LogFileKey, err := w.processFile(workerID, task, baseArgs, file)

		mu.Lock()
		*processedFiles = append(*processedFiles, file)

		if err != nil {
			// Record failed file
			*failedFiles = append(*failedFiles, file)

			// Create detailed error
			detailedErr := fmt.Errorf("worker %d failed to process file %s: %v", workerID, file, err)

			// Send error to channel
			select {
			case errChan <- detailedErr:
				// Error sent successfully
			default:
				// Error channel is full, log and continue
				log.Errorf("Error channel full, could not send error: %v", detailedErr)
			}

			log.Errorf("Worker %d: Failed to process file %s: %v", workerID, file, err)
		} else {
			// Add to log files list if successful
			*logFiles = append(*logFiles, s3LogFileKey)
			log.Debugf("Worker %d: Successfully processed file %s", workerID, file)
		}

		// Report progress every 10 files
		if len(*processedFiles)%10 == 0 {
			log.Infof("Progress: %d files processed, %d failed",
				len(*processedFiles), len(*failedFiles))
		}

		mu.Unlock()
	}

	log.Infof("Worker %d: Finished processing", workerID)
}

// processFile processes a single file with rclone
func (w *Worker) processFile(workerID int, task *common.MigrationTask, baseArgs []string,
	file string) (string, error) {

	// Create unique log file
	logFile := fmt.Sprintf("/tmp/rclone_copy_%d_%d_worker%d_%s.log",
		task.Timestamp, task.ID, workerID, sanitizeFileName(filepath.Base(file)))

	// Prepare arguments
	args := make([]string, len(baseArgs))
	copy(args, baseArgs)
	args = append(args, "--log-file", logFile, "--no-traverse", "--files-from", file)

	log.Infof("Worker %d: Executing rclone for file %s with rclone args: %v", workerID, file, args)

	// Create and execute command
	cmd := exec.Command("rclone", args...)
	output, err := cmd.CombinedOutput()

	if err != nil {
		// Return detailed error including command output
		return "", fmt.Errorf("rclone command failed: %v, output: %s", err, string(output))
	}

	log.Infof("Worker %d: Successfully processed file %s", workerID, file)

	// Upload log file
	s3LogFileKey := fmt.Sprintf("rclone/log-files/%d/%s", task.Timestamp, filepath.Base(logFile))
	if uploadErr := w.uploadLogFile(logFile, task, s3LogFileKey); uploadErr != nil {
		log.Warningf("Worker %d: Failed to upload log file %s: %v", workerID, logFile, uploadErr)
		// Don't fail the entire operation if upload fails
		// The file was still processed successfully
	}

	return s3LogFileKey, nil
}

// sanitizeFileName removes invalid characters from file name
func sanitizeFileName(name string) string {
	// Replace problematic characters
	invalidChars := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|"}
	result := name
	for _, char := range invalidChars {
		result = strings.ReplaceAll(result, char, "_")
	}
	return result
}
