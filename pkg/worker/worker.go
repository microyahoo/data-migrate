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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	log "github.com/sirupsen/logrus"

	"github.com/microyahoo/data-migrate/pkg/common"
)

type Worker struct {
	serverAddr string
	clientID   string
}

func NewWorker(serverAddr string) *Worker {
	hostname, _ := os.Hostname()
	return &Worker{
		serverAddr: serverAddr,
		clientID:   fmt.Sprintf("%s-%d", hostname, os.Getpid()),
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

func (w *Worker) checkTask(task *common.MigrationTask) error {
	if task.SourceDir == "" || task.TargetDir == "" {
		return fmt.Errorf("Source or destination is empty")
	}
	st, err := common.GetFilesystemType(task.SourceDir)
	if err != nil {
		return err
	}
	_, err = os.Stat(task.TargetDir)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(task.TargetDir, 0755); err != nil {
				return err
			}
		} else {
			return fmt.Errorf("failed to stat target dir %s: %s", task.TargetDir, err)
		}
	}
	tt, err := common.GetFilesystemType(task.TargetDir)
	if err != nil {
		return err
	}
	if st != task.SourceFsType || tt != task.TargetFsType {
		return fmt.Errorf("actual source or target filesystem type not match(%s, %s)", st, tt)
	}
	si, err := os.Stat(task.SourceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source dir %s not exists", task.SourceDir)
		} else {
			return fmt.Errorf("failed to stat source dir %s: %s", task.SourceDir, err)
		}
	} else if !si.IsDir() {
		return fmt.Errorf("source dir %s is not directory", task.SourceDir)
	}
	if task.FileListPath == "" {
		// not specify file list path
		return nil
	}
	fi, err := os.Stat(task.FileListPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file list path %s not exists", task.FileListPath)
		} else {
			return fmt.Errorf("failed to stat file list path %s: %s", task.FileListPath, err)
		}
	} else if fi.IsDir() {
		return fmt.Errorf("file list path %s is directory", task.FileListPath)
	}
	return nil
}

func (w *Worker) loadDirectories(task *common.MigrationTask) (includeFile string, s3IncludeFileKey string, err error) {
	if task.FileListPath == "" {
		return "", "", nil
	}
	// read original migration files or directory lists
	file, err := os.Open(task.FileListPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to open list file: %v", err)
	}
	defer file.Close()

	var entries []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		dir := strings.TrimSpace(scanner.Text())
		if dir != "" {
			entries = append(entries, dir)
		}
	}

	if err := scanner.Err(); err != nil {
		return "", "", fmt.Errorf("error reading list file: %v", err)
	}

	// create a temp file for include-from file
	tempFile, err := os.CreateTemp("", fmt.Sprintf("include_%d_*.txt", task.ID))
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp file: %v", err)
	}
	defer tempFile.Close()

	for _, path := range entries {
		var includePattern string

		if task.CheckSourceEntry {
			fullPath := filepath.Join(task.SourceDir, path)

			// check file or directory
			info, err := os.Stat(fullPath)
			if err != nil {
				log.Errorf("Warning: Cannot stat %s: %v", fullPath, err)
				continue
			}

			if info.IsDir() {
				// directories: add prefix / and append /** suffix
				includePattern = fmt.Sprintf("/%s/**", strings.TrimPrefix(path, "/"))
			} else {
				// file: only add prefix /
				includePattern = fmt.Sprintf("/%s", strings.TrimPrefix(path, "/"))
			}
		} else {
			log.Debug("If not checking each entry, treat all as directories")
			includePattern = fmt.Sprintf("/%s/**", strings.TrimPrefix(path, "/"))
		}

		// write include patterns to temp file
		if _, err := tempFile.WriteString(includePattern + "\n"); err != nil {
			return "", "", fmt.Errorf("failed to write to temp file: %v", err)
		}
	}

	includeFile = tempFile.Name()
	log.Infof("Created include file for task %d: %s with %d patterns",
		task.ID, includeFile, len(entries))

	s3IncludeFileKey = fmt.Sprintf("rclone/include-files/%d/%s", task.Timestamp,
		filepath.Base(includeFile))
	if e := w.uploadFile(tempFile, task.Bucket, task.S3Config, s3IncludeFileKey); e != nil {
		log.Warningf("failed to upload include file %s to s3: %s", includeFile, e)
	}

	return includeFile, s3IncludeFileKey, nil
}

func (w *Worker) uploadFile(f *os.File, bucket string, s3Config *common.S3Configuration, key string) error {
	log.Infof("Start to upload results %s to s3 endpoint %s:%s", key, s3Config.Endpoint, bucket)
	ctx := context.Background()
	client, err := common.NewS3Client(ctx, s3Config.Endpoint, s3Config.AccessKey, s3Config.SecretKey,
		s3Config.Region, s3Config.SkipSSLVerify)
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
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
	}

	if err := w.checkTask(task); err != nil {
		result.Success = false
		result.Message = err.Error()
		return result
	}

	log.Printf("Executing task %d: %s -> %s",
		task.ID, task.SourceDir, task.TargetDir)

	includeFile, s3IncludeFileKey, err := w.loadDirectories(task)
	if err != nil {
		result.Success = false
		result.Message = err.Error()
		return result
	}

	// TODO: CreateTemp
	logFile := fmt.Sprintf("/tmp/rclone_copy_%d_%d.txt", task.Timestamp, task.ID)

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
	if includeFile != "" {
		args = append(args, "--include-from", includeFile)
	}
	args = append(args, "--log-file", logFile)

	// --checkers 128 --transfers 128 --size-only --local-no-set-modtime --log-level INFO --log-file <log-file>
	cmd := exec.Command("rclone", args...)
	_, err = cmd.CombinedOutput()

	var s3LogFileKey string
	f, e := os.Open(logFile)
	if e == nil {
		s3LogFileKey = fmt.Sprintf("rclone/log-files/%d/%s", task.Timestamp, filepath.Base(logFile))
		if e := w.uploadFile(f, task.Bucket, task.S3Config, s3LogFileKey); e != nil {
			log.Warningf("failed to upload rclone log file %s to s3: %s", logFile, e)
		}
	} else {
		log.Warningf("failed to open rclone log file %s: %s", logFile, e)
	}

	result.IncludeFile = s3IncludeFileKey
	result.LogFile = s3LogFileKey
	if err != nil {
		result.Success = false
		result.Message = err.Error()
	} else {
		result.Success = true
		result.Message = fmt.Sprintf("Migrated task %d with include file %s successfully",
			task.ID, includeFile)
	}

	return result
}
