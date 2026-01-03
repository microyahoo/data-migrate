package server

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	log "github.com/sirupsen/logrus"

	"github.com/microyahoo/data-migrate/pkg/common"
)

type Server struct {
	config *common.MigrationConf
	tasks  []*common.MigrationTask // defined in config file

	pendingTasks     chan common.MigrationTask
	completed        chan common.TaskResult
	results          []common.TaskResult
	clients          map[string]net.Conn
	mu               sync.RWMutex
	done             chan struct{}
	completedCounter uint64
	successCounter   uint64
	failedCounter    uint64

	serverSideListing bool
	pendingSubTasks   *sync.Map // task id -> subtask chan
	subTaskCounterMap *sync.Map // task id -> counter
}

func NewServer(cfgFile string) (*Server, error) {
	config := common.LoadConfigFromFile(cfgFile)
	common.CheckSetConfig(config)
	tasks, err := common.ParseTaskFile(config)
	if err != nil {
		return nil, err
	}
	server := &Server{
		config:  config,
		clients: make(map[string]net.Conn),
		done:    make(chan struct{}),
	}
	if config.GlobalConfig.UltraLargeScale && config.GlobalConfig.ServerSideListing {
		var validTasks []*common.MigrationTask
		for _, task := range tasks {
			if err := task.Check(); err != nil {
				log.Warningf("task %d is invalid, source dir: %s, target dir: %s, error: %s", task.ID, task.SourceDir, task.TargetDir, err)
			} else {
				task.ID = len(validTasks) + 1 // 0 means no more tasks
				validTasks = append(validTasks, task)
			}
		}
		server.tasks = validTasks
		// server.pendingTasks = make(chan common.MigrationTask, len(validTasks))
		server.completed = make(chan common.TaskResult, len(validTasks))
		server.serverSideListing = true
		server.pendingSubTasks = new(sync.Map)
		server.subTaskCounterMap = new(sync.Map)
	} else {
		server.tasks = tasks
		server.pendingTasks = make(chan common.MigrationTask, len(tasks))
		server.completed = make(chan common.TaskResult, len(tasks))

		for _, task := range server.tasks {
			server.pendingTasks <- *task
		}
	}
	return server, nil
}

func (s *Server) Start(serverPort int) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	defer listener.Close()

	log.Infof("Server started on :%d", serverPort)

	go s.handleResults()
	if s.serverSideListing {
		go s.handleServerListing()
	}

	for {
		select {
		case <-s.done:
			log.Infof("All data migration tasks done!")
			return nil
		default:
			conn, err := listener.Accept()
			if err != nil {
				log.Errorf("Error accepting connection: %v", err)
				continue
			}

			clientID := conn.RemoteAddr().String()
			log.Infof("New client connected: %s", clientID)

			s.mu.Lock()
			s.clients[clientID] = conn
			s.mu.Unlock()

			go s.handleClient(conn, clientID)
		}
	}
}

func (s *Server) handleServerListing() error {
	hostname, _ := os.Hostname()
	serverID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	for i, task := range s.tasks {
		filesDir := filepath.Join(task.FileListDir, fmt.Sprintf("%d", task.Timestamp), serverID)
		if err := os.MkdirAll(filesDir, 0755); err != nil {
			return fmt.Errorf("failed to create output directory: %v", err)
		}

		outputPrefix := filepath.Base(task.FileListPath)
		if task.FileListPath == "" {
			outputPrefix = filepath.Base(task.SourceDir)
		}
		// Get file channel from FindFiles
		fileChan, err := common.FindFiles(task.SourceDir, task.TargetDir, task.FileListPath,
			0,            /* list concurrency (let FindFiles decide) */
			filesDir,     /*output directory*/
			outputPrefix, /*output prefix*/
			task.MaxFilesPerOutput /*max files per output*/)
		if err != nil {
			return fmt.Errorf("failed to find files: %v", err)
		}
		go s.collectFileList(i, fileChan)
	}
	return nil
}

func (s *Server) collectFileList(index int, fileChan <-chan string) {
	var (
		task    = s.tasks[index]
		counter = 0
		taskID  = task.ID
	)
	for {
		f, ok := <-fileChan
		if !ok {
			s.subTaskCounterMap.Store(taskID, counter)
			return
		}
		subTask, err := task.DeepCopyJSON()
		if err != nil {
			panic(err)
		}
		subTask.SubTaskID = counter + 1 // start from 1
		subTask.FileFrom = f

		if subTaskChanV, ok := s.pendingSubTasks.Load(taskID); ok {
			subTaskChan := subTaskChanV.(chan common.MigrationTask)
			subTaskChan <- *subTask
		} else {
			subTaskChan := make(chan common.MigrationTask, 1024) // 1024 is enough
			subTaskChan <- *subTask
			s.pendingSubTasks.Store(taskID, subTaskChan)
		}
		counter++
	}
}

func (s *Server) handleClient(conn net.Conn, clientID string) {
	defer func() {
		conn.Close()
		s.mu.Lock()
		delete(s.clients, clientID)
		s.mu.Unlock()
		log.Infof("Client disconnected: %s", clientID)
	}()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var req common.TaskRequest
		if err := decoder.Decode(&req); err != nil {
			if err != io.EOF {
				log.Errorf("Error decoding request from %s: %v", clientID, err)
			}
			return
		}

		req.ClientID = clientID
		log.Infof("Received task request from client %s", clientID)

		var (
			start              = time.Now()
			taskToSend         *common.MigrationTask
			currentSubTaskChan chan common.MigrationTask
		)
		if s.serverSideListing {
			// Iterate through all tasks to find one with available subtasks
			for {
				for _, task := range s.tasks {
					taskID := task.ID
					// Check if subtask channel exists for this task
					if subTaskChanV, ok := s.pendingSubTasks.Load(taskID); ok {
						subTaskChan := subTaskChanV.(chan common.MigrationTask)

						// Check if there are subtasks available in the channel
						select {
						case subTask := <-subTaskChan:
							// Found an available subtask
							taskToSend = &subTask
							currentSubTaskChan = subTaskChan

							log.Infof("Assigned task %d to client %s, sending subtask %d",
								taskID, clientID, subTask.SubTaskID)
						default:
							// Channel is empty, check if all subtasks have been sended/completed
							if _, ok := s.subTaskCounterMap.Load(taskID); ok {
								log.Debugf("Task %d already sended all subtasks", taskID)
								s.pendingSubTasks.Delete(taskID)
							}
						}
					} else {
						if _, ok := s.subTaskCounterMap.Load(taskID); ok {
							log.Debugf("Task %d has been removed from pending tasks", taskID)
						} else {
							// file list is still being generated
							log.Infof("Task %d file list is still being generated", taskID)
						}
						continue
					}
					if taskToSend != nil {
						break
					}
				}

				// If an available subtask was found
				if taskToSend != nil {
					break
				}
				// Check if all tasks have been completed
				allTasksDone := true
				for _, task := range s.tasks {
					taskID := task.ID
					if _, ok := s.subTaskCounterMap.Load(taskID); !ok {
						allTasksDone = false
						break
					}
				}

				if allTasksDone {
					log.Infof("All tasks completed, closing connection with client %s", clientID)
					if err := encoder.Encode(common.MigrationTask{ID: 0}); err != nil {
						log.Errorf("Error sending completion to client %s: %v", clientID, err)
					}
					return
				}

				// No available subtasks at the moment, wait a bit
				log.Infof("No available subtasks for client %s, waiting...", clientID)
				time.Sleep(2 * time.Second)
			}
		} else {
			select {
			case task := <-s.pendingTasks:
				taskToSend = &task
			default:
				// no more tasks
				log.Infof("No more tasks for client %s", clientID)
				if err := encoder.Encode(common.MigrationTask{ID: 0}); err != nil {
					log.Errorf("Error sending no-task to client %s: %v", clientID, err)
				}
				return
			}
		}
		if err := encoder.Encode(*taskToSend); err != nil {
			log.Errorf("Error sending task to client %s: %v", clientID, err)
			// re-push to pending tasks
			if s.serverSideListing {
				currentSubTaskChan <- *taskToSend
			} else {
				s.pendingTasks <- *taskToSend
			}
			return
		} else if s.serverSideListing {
			log.Infof("Sending task %d(%d) with %s file from %s to client %s", taskToSend.ID,
				taskToSend.SubTaskID, taskToSend.SourceDir, taskToSend.FileFrom, clientID)
		} else {
			log.Infof("Sending task %d with %s to client %s", taskToSend.ID, taskToSend.SourceDir, clientID)
		}
		var result common.TaskResult
		if err := decoder.Decode(&result); err != nil {
			if err != io.EOF {
				log.Errorf("Error decoding request from %s: %v", clientID, err)
			}
			return
		}
		result.StartTime = start
		result.EndTime = time.Now()
		result.Duration = time.Since(start)
		result.ClientID = clientID
		if result.Success {
			if s.serverSideListing {
				log.Infof("task(%d/%d) with file from %s has been migrated from %s to %s successfully",
					taskToSend.ID, taskToSend.SubTaskID, taskToSend.FileFrom, taskToSend.SourceDir, taskToSend.TargetDir)
			} else {
				log.Infof("task(%d) has been migrated from %s to %s successfully", taskToSend.ID, taskToSend.SourceDir, taskToSend.TargetDir)
			}
		} else {
			if s.serverSideListing {
				log.Errorf("failed to migrate task(%d/%d) with file from %s from %s to %s", taskToSend.ID,
					taskToSend.SubTaskID, taskToSend.FileFrom, taskToSend.SourceDir, taskToSend.TargetDir)
			} else {
				log.Errorf("failed to migrate task(%d) from %s to %s", taskToSend.ID, taskToSend.SourceDir, taskToSend.TargetDir)
			}
		}
		s.completed <- result
	}
}

func (s *Server) handleResults() {
	var (
		taskStartTimeMap = make(map[int]time.Time)
		taskEndTimeMap   = make(map[int]time.Time)
		taskCounterMap   = make(map[int]int)
		taskResultsMap   = make(map[int][]common.TaskResult)
		failedTaskMap    = make(map[int][]string)
	)
	for result := range s.completed {
		var lastResult *common.TaskResult
		if t, ok := taskStartTimeMap[result.TaskID]; !ok {
			taskStartTimeMap[result.TaskID] = result.StartTime
			taskEndTimeMap[result.TaskID] = result.EndTime
		} else {
			if t.After(result.StartTime) {
				taskStartTimeMap[result.TaskID] = result.StartTime
			}
			if taskEndTimeMap[result.TaskID].Before(result.EndTime) {
				taskEndTimeMap[result.TaskID] = result.EndTime
			}
		}
		if !result.Success {
			atomic.AddUint64(&s.failedCounter, 1)
			failedTaskMap[result.TaskID] = append(failedTaskMap[result.TaskID], result.Message)
		} else {
			atomic.AddUint64(&s.successCounter, 1)
		}
		taskCounterMap[result.TaskID] = taskCounterMap[result.TaskID] + 1
		taskResultsMap[result.TaskID] = append(taskResultsMap[result.TaskID], result)
		info := fmt.Sprintf("Task %d completed by client %s: success: %v, message: %s",
			result.TaskID, result.ClientID, result.Success, result.Message)
		if s.serverSideListing {
			info = fmt.Sprintf("Task %d/%d with file from %s completed by client %s: success: %v, message: %s",
				result.TaskID, result.SubTaskID, result.FileFrom, result.ClientID, result.Success, result.Message)
		}
		log.Info(info)

		if s.serverSideListing {
			counter, ok := s.subTaskCounterMap.Load(result.TaskID)
			if ok && taskCounterMap[result.TaskID] == counter {
				atomic.AddUint64(&s.completedCounter, 1)
				lastResult = &common.TaskResult{
					SourceDir: result.SourceDir,
					TargetDir: result.TargetDir,
					TaskID:    result.TaskID,
					Success:   true,
					StartTime: taskStartTimeMap[result.TaskID],
					EndTime:   taskEndTimeMap[result.TaskID],
					Duration:  taskEndTimeMap[result.TaskID].Sub(taskStartTimeMap[result.TaskID]),
				}
				failedMessages := failedTaskMap[result.TaskID]
				if len(failedMessages) > 0 {
					lastResult.Success = false
					if len(failedMessages) == 1 {
						lastResult.Message = fmt.Sprintf("1 file failed: %v", failedMessages[0])
					} else {

						// Create a summary error message
						errorSummary := fmt.Sprintf("%d files failed. First few errors:", len(failedMessages))
						for i := 0; i < len(failedMessages) && i < 3; i++ {
							errorSummary += fmt.Sprintf("\n- %v", failedMessages[i])
						}
						if len(failedMessages) > 3 {
							errorSummary += fmt.Sprintf("\n... and %d more errors", len(failedMessages)-3)
						}
						lastResult.Message = errorSummary
					}
				}
			}
		} else {
			atomic.AddUint64(&s.completedCounter, 1)
		}

		if result.LogFile != "" {
			result.LogFile = filepath.Join(s.config.ReportConfig.S3Config.Endpoint, s.config.ReportConfig.Bucket, result.LogFile)
		}
		s.writeResultToCSV(result)
		s.results = append(s.results, result)

		if s.serverSideListing && lastResult != nil {
			s.writeResultToCSV(*lastResult)
			s.results = append(s.results, *lastResult)
		}

		if s.config.GlobalConfig.FeishuURL != "" {
			var content string
			if s.serverSideListing {
				if lastResult != nil {
					content = NewFormatter().FormatMigrationMessage(*lastResult)
				}
			} else {
				content = NewFormatter().FormatMigrationMessage(result)
			}
			err := common.SendFeishuCard(
				s.config.GlobalConfig.FeishuURL,
				content,
			)
			if err != nil {
				log.Warningf("send feishu message failed: %v", err)
			} else {
				log.Infof("send feishu message successfully")
			}
		}
		completed := atomic.LoadUint64(&s.completedCounter)
		if int(completed) == len(s.tasks) {
			s.generateResults(s.results)
			close(s.done)
		}
	}
}

func (s *Server) sendSummary(key string) {
	if s.config.GlobalConfig.FeishuURL == "" {
		return
	}
	total := len(s.tasks)
	success := atomic.LoadUint64(&s.successCounter)
	failed := atomic.LoadUint64(&s.failedCounter)
	content := NewFormatter().FormatTotalResults(total, int(success), int(failed), key)
	err := common.SendFeishuCard(
		s.config.GlobalConfig.FeishuURL,
		content,
	)
	if err != nil {
		log.Warningf("send feishu message failed: %v", err)
	} else {
		log.Infof("send feishu message successfully")
	}
}

func (s *Server) GetProgress() (int, int, int, int, []string) {
	var clients []string
	s.mu.Lock()
	for clientID := range s.clients {
		clients = append(clients, clientID)
	}
	s.mu.Unlock()
	total := len(s.tasks)
	completed := atomic.LoadUint64(&s.completedCounter)
	success := atomic.LoadUint64(&s.successCounter)
	failed := atomic.LoadUint64(&s.failedCounter)

	slices.Sort(clients)
	return int(completed), total, int(success), int(failed), clients
}

func (s *Server) writeResultToCSV(result common.TaskResult) {
	file, created, err := s.getCSVFileHandle()
	if err != nil {
		log.WithError(err).Error("Could not get a file handle for the CSV results")
		return
	}
	defer file.Close()

	csvwriter := csv.NewWriter(file)

	if created {
		err = csvwriter.Write([]string{
			"id",
			"subtask id",
			"source dir",
			"target dir",
			"client id",
			"duration",
			"success",
			"split pattern",
			"split files",
			"file from",
			"log file",
			"message",
		})
		if err != nil {
			log.WithError(err).Error("failed writing line to results csv")
			return
		}
	}

	err = csvwriter.Write([]string{
		fmt.Sprintf("%d", result.TaskID),
		fmt.Sprintf("%d", result.SubTaskID),
		result.SourceDir,
		result.TargetDir,
		result.ClientID,
		fmt.Sprintf("%s", result.Duration),
		fmt.Sprintf("%t", result.Success),
		result.SplitPattern,
		fmt.Sprintf("%d", result.SplitFiles),
		result.FileFrom,
		result.LogFile,
		result.Message,
	})
	if err != nil {
		log.WithError(err).Error("Failed writing line to results csv")
		return
	}

	csvwriter.Flush()

}

func (s *Server) getCSVFileHandle() (*os.File, bool, error) {
	file, err := os.OpenFile("data_migrate_results.csv", os.O_APPEND|os.O_WRONLY, 0755)
	if err == nil {
		return file, false, nil
	}
	file, err = os.OpenFile("/tmp/data_migrate_results.csv", os.O_APPEND|os.O_WRONLY, 0755)
	if err == nil {
		return file, false, nil
	}

	file, err = os.OpenFile("data_migrate_results.csv", os.O_WRONLY|os.O_CREATE, 0755)
	if err == nil {
		return file, true, nil
	}
	file, err = os.OpenFile("/tmp/data_migrate_results.csv", os.O_WRONLY|os.O_CREATE, 0755)
	if err == nil {
		return file, true, nil
	}

	return nil, false, errors.New("Could not find previous CSV for appending and could not write new CSV file to current dir and /tmp/ giving up")

}

func (s *Server) generateResults(results []common.TaskResult) {
	t := table.NewWriter()
	var format = "csv"
	if s.config.ReportConfig != nil {
		format = s.config.ReportConfig.Format
	}
	var timestamp = time.Now().UnixMilli()
	outputFile := fmt.Sprintf("/tmp/data_migrate_results_%d.%s", timestamp, format)
	log.Infof("Start to generate data migration results to %s", outputFile)
	{
		f, err := os.Create(outputFile)
		if err != nil {
			log.Warningf("Failed to open file %s: %s", outputFile, err)
			t.SetOutputMirror(os.Stdout)
		} else {
			defer f.Close()
			t.SetOutputMirror(f)
		}
		t.AppendHeader(table.Row{
			"id",
			"subtask id",
			"source dir",
			"target dir",
			"client id",
			"duration",
			"success",
			"split pattern",
			"split files",
			"file from",
			"log file",
			"message",
		})
		for _, result := range results {
			t.AppendRow(table.Row{
				fmt.Sprintf("%d", result.TaskID),
				fmt.Sprintf("%d", result.SubTaskID),
				result.SourceDir,
				result.TargetDir,
				result.ClientID,
				fmt.Sprintf("%s", result.Duration),
				fmt.Sprintf("%t", result.Success),
				result.SplitPattern,
				fmt.Sprintf("%d", result.SplitFiles),
				result.FileFrom,
				result.LogFile,
				result.Message,
			})
		}
		t.SetColumnConfigs([]table.ColumnConfig{
			{Number: 1, Align: text.AlignCenter},
			{Number: 2, Align: text.AlignCenter},
			{Number: 3, Align: text.AlignCenter},
			{Number: 4, Align: text.AlignCenter},
			{Number: 5, Align: text.AlignCenter},
			{Number: 6, Align: text.AlignCenter},
			{Number: 7, Align: text.AlignCenter},
			{Number: 8, Align: text.AlignCenter},
			{Number: 9, Align: text.AlignCenter},
		})
		t.SortBy([]table.SortBy{
			{
				Name: "id",
				Mode: table.AscNumericAlpha,
			},
		})
		switch strings.ToLower(format) {
		case "md", "markdown":
			t.RenderMarkdown()
		case "csv":
			t.RenderCSV()
		case "html":
			t.RenderHTML()
		default:
			t.Render()
		}
	}

	if s.config.ReportConfig == nil || s.config.ReportConfig.Bucket == "" || s.config.ReportConfig.S3Config == nil {
		return
	}

	s3Config := s.config.ReportConfig.S3Config
	log.Infof("Start to upload data migration results to s3 endpoint %s", s3Config.Endpoint)
	ctx := context.Background()
	client, e := common.NewS3Client(ctx, s3Config.Endpoint, s3Config.AccessKey, s3Config.SecretKey,
		s3Config.Region, s3Config.SkipSSLVerify)
	if e != nil {
		log.WithError(e).Warning("Unable to build S3 config to upload report")
		return
	}
	f, e := os.Open(outputFile)
	if e != nil {
		log.WithError(e).Warning("Unable to open report file")
		return
	}
	key := fmt.Sprintf("rclone/reports/data_migrate_results_%d.%s", timestamp, format)
	defer f.Close()
	_, e = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.config.ReportConfig.Bucket,
		ACL:    types.ObjectCannedACLPublicRead,
		Key:    aws.String(key),
		Body:   f,
	})
	if e != nil {
		log.WithError(e).Warningf("Failed to upload report file to s3 bucket %s", s.config.ReportConfig.Bucket)
	}
	s.sendSummary(filepath.Join(s.config.ReportConfig.S3Config.Endpoint, s.config.ReportConfig.Bucket, key))
}

// Formatter
type Formatter struct {
	SeparatorChar string
	SeparatorLen  int
}

func NewFormatter() *Formatter {
	return &Formatter{
		SeparatorChar: "*",
		SeparatorLen:  50,
	}
}

func (f *Formatter) writeLine(builder *strings.Builder, label, value string) {
	builder.WriteString(fmt.Sprintf("%s: %s", label, value) + "\n")
}

func (f *Formatter) FormatMigrationMessage(msg common.TaskResult) string {
	var builder strings.Builder

	status := "成功"
	if !msg.Success {
		status = "失败"
	}
	titleLine := fmt.Sprintf("迁移任务状态: %s", status)
	builder.WriteString(titleLine + "\n")

	separator := strings.Repeat(f.SeparatorChar, f.SeparatorLen)
	builder.WriteString(separator + "\n")

	f.writeLine(&builder, "source dir", msg.SourceDir)
	f.writeLine(&builder, "target dir", msg.TargetDir)
	f.writeLine(&builder, "duration", fmt.Sprintf("%s", msg.Duration))
	f.writeLine(&builder, "task id", fmt.Sprintf("%d", msg.TaskID))
	f.writeLine(&builder, "client id", msg.ClientID)
	f.writeLine(&builder, "split pattern", msg.SplitPattern)
	f.writeLine(&builder, "split files", fmt.Sprintf("%d", msg.SplitFiles))
	f.writeLine(&builder, "log file", msg.LogFile)
	f.writeLine(&builder, "message", msg.Message)

	builder.WriteString(separator + "\n")

	return builder.String()
}

func (f *Formatter) FormatTotalResults(total, success, failed int, key string) string {
	var builder strings.Builder

	titleLine := fmt.Sprintf("迁移结果汇总")
	builder.WriteString(titleLine + "\n")

	separator := strings.Repeat(f.SeparatorChar, f.SeparatorLen)
	builder.WriteString(separator + "\n")

	f.writeLine(&builder, "total", fmt.Sprintf("%d", total))
	f.writeLine(&builder, "success", fmt.Sprintf("%d", success))
	f.writeLine(&builder, "failed", fmt.Sprintf("%d", failed))
	f.writeLine(&builder, "report", key)
	builder.WriteString(separator + "\n")

	return builder.String()
}
