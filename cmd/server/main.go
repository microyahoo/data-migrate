package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/microyahoo/data-migrate/pkg/common"
)

var (
	configFile   string
	serverPort   int
	debug, trace bool
)

func main() {
	rootCmd := newCommand()
	cobra.CheckErr(rootCmd.Execute())
}

func newCommand() *cobra.Command {
	cmds := &cobra.Command{
		Use: "data-migrate-server",
		Run: func(cmd *cobra.Command, args []string) {
			run()
		},
	}
	cmds.Flags().SortFlags = false

	viper.SetDefault("DEBUG", false)
	viper.SetDefault("TRACE", false)
	viper.SetDefault("SERVERPORT", 2000)

	viper.AutomaticEnv()
	viper.AllowEmptyEnv(true)

	cmds.Flags().StringVar(&configFile, "config.file", viper.GetString("CONFIGFILE"), "Config file describing test run")
	cmds.Flags().BoolVar(&debug, "debug", viper.GetBool("DEBUG"), "enable debug log output")
	cmds.Flags().BoolVar(&trace, "trace", viper.GetBool("TRACE"), "enable trace log output")
	cmds.Flags().IntVar(&serverPort, "server.port", viper.GetInt("SERVERPORT"), "Port on which the server will be available for clients. Default: 2000")

	// cmds.MarkFlagRequired("config.file")

	return cmds
}

func run() {
	if configFile == "" {
		log.Fatal("--config.file is a mandatory parameter - please specify the config file")
	}
	if debug {
		log.SetLevel(log.DebugLevel)
	} else if trace {
		log.SetLevel(log.TraceLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	log.Debugf("viper settings=%+v", viper.AllSettings())
	log.Debugf("data-migrate server configFile=%s, serverPort=%d", configFile, serverPort)

	server, err := NewServer(configFile)
	if err != nil {
		log.Fatalf("failed to create server", err)
	}
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			completed, total := server.GetProgress()
			log.Infof("Progress: %d/%d tasks completed", completed, total)
		}
	}()
	server.Start()
}

type Server struct {
	config       *common.MigrationConf
	tasks        []*common.MigrationTask
	pendingTasks chan common.MigrationTask
	completed    chan common.TaskResult
	results      []common.TaskResult
	clients      map[string]net.Conn
	mu           sync.RWMutex
	done         chan struct{}
}

func NewServer(cfgFile string) (*Server, error) {
	config := common.LoadConfigFromFile(cfgFile)
	common.CheckSetConfig(config)
	tasks, err := common.ParseTaskFile(config.GlobalConfig.TasksFile)
	if err != nil {
		return nil, err
	}
	server := &Server{
		config:       config,
		tasks:        tasks,
		pendingTasks: make(chan common.MigrationTask, len(tasks)),
		completed:    make(chan common.TaskResult, len(tasks)),
		clients:      make(map[string]net.Conn),
		done:         make(chan struct{}),
	}
	timestamp := time.Now().UnixMilli()
	for i := range tasks {
		// tasks[i].CheckSourceEntry = config.GlobalConfig.CheckSourceEntry
		tasks[i].SourceFsType = config.GlobalConfig.SourceFsType
		tasks[i].TargetFsType = config.GlobalConfig.TargetFsType
		tasks[i].Bucket = config.ReportConfig.Bucket
		tasks[i].RcloneFlags = config.GlobalConfig.RcloneFlags
		tasks[i].S3Config = config.ReportConfig.S3Config
		tasks[i].Timestamp = timestamp

		server.pendingTasks <- *tasks[i]
	}
	return server, nil
}

func (s *Server) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", serverPort))
	if err != nil {
		return fmt.Errorf("failed to start server: %v", err)
	}
	defer listener.Close()

	log.Infof("Server started on :%d", serverPort)

	go s.handleResults()

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
			task     common.MigrationTask
			duration time.Duration
			start    = time.Now()
		)
		select {
		case task = <-s.pendingTasks:
			log.Infof("Sending task %d to client %s", task.ID, clientID)
			if err := encoder.Encode(task); err != nil {
				log.Errorf("Error sending task to client %s: %v", clientID, err)
				// re-push to pending tasks
				s.pendingTasks <- task
				return
			}
		default:
			// no more tasks
			log.Infof("No more tasks for client %s", clientID)
			if err := encoder.Encode(common.MigrationTask{ID: 0}); err != nil {
				log.Errorf("Error sending no-task to client %s: %v", clientID, err)
			}
			return
		}
		var result common.TaskResult
		if err := decoder.Decode(&result); err != nil {
			if err != io.EOF {
				log.Errorf("Error decoding request from %s: %v", clientID, err)
			}
			return
		}
		duration = time.Since(start)
		result.Duration = duration
		result.ClientID = clientID
		if result.Success {
			log.Infof("task(%d) has been migrated from %s to %s successfully", task.ID, task.SourceDir, task.TargetDir)
		} else {
			log.Errorf("failed to migrate task(%d) from %s to %s", task.ID, task.SourceDir, task.TargetDir)
		}
		s.completed <- result
	}
}

func (s *Server) handleResults() {
	for result := range s.completed {
		log.Printf("Task %d completed by client %s: success=%v, message=%s",
			result.TaskID, result.ClientID, result.Success, result.Message)

		s.writeResultToCSV(result)
		s.results = append(s.results, result)
		// if !result.Success {
		// TODO
		// 	log.Printf("Task %d failed, will retry later", result.TaskID)
		// }
		if len(s.results) == len(s.tasks) {
			s.generateResults(s.results)
			close(s.done)
		}
	}
}

func (s *Server) GetProgress() (int, int) {
	total := len(s.tasks)
	completed := total - len(s.pendingTasks)
	return completed, total
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
			"source dir",
			"target dir",
			"client id",
			"duration",
			"success",
			"include file",
			"log file",
			"message",
		})
		if err != nil {
			log.WithError(err).Error("failed writing line to results csv")
			return
		}
	}

	err = csvwriter.Write([]string{
		fmt.Sprintf("%s", result.TaskID),
		result.SourceDir,
		result.TargetDir,
		result.ClientID,
		fmt.Sprintf("%s", result.Duration),
		fmt.Sprintf("%t", result.Success),
		result.IncludeFile,
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
	log.Infof("Start to generate benchmark results to %s", outputFile)
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
			"source dir",
			"target dir",
			"client id",
			"duration",
			"success",
			"include file",
			"log file",
			"message",
		})
		for _, result := range results {
			t.AppendRow(table.Row{
				fmt.Sprintf("%s", result.TaskID),
				result.SourceDir,
				result.TargetDir,
				result.ClientID,
				fmt.Sprintf("%s", result.Duration),
				fmt.Sprintf("%t", result.Success),
				result.IncludeFile,
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
				Mode: table.Asc,
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
	log.Infof("Start to upload benchmark results to s3 endpoint %s", s3Config.Endpoint)
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
	defer f.Close()
	_, e = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.config.ReportConfig.Bucket,
		Key:    aws.String(fmt.Sprintf("data_migrate_results_%d.%s", timestamp, format)),
		Body:   f,
	})
	if e != nil {
		log.WithError(e).Warningf("Failed to upload report file to s3 bucket %s", s.config.ReportConfig.Bucket)
	}
}

func init() {
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})
	rand.New(rand.NewSource(time.Now().UnixNano()))
}
