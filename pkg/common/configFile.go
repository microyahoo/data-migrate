package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	log "github.com/sirupsen/logrus"
)

type RcloneFlags struct {
	Checkers          int    `yaml:"checkers" json:"checkers"`
	Transfers         int    `yaml:"transfers" json:"transfers"`
	LogLevel          string `yaml:"log_level" json:"log_level"`
	LocalNoSetModtime bool   `yaml:"local_no_set_modtime" json:"local_no_set_modtime"`
	SizeOnly          bool   `yaml:"size_only" json:"size_only"`
	Dryrun            bool   `yaml:"dryrun" json:"dryrun"`
}

// S3Configuration contains all information to connect to a certain S3 endpoint
type S3Configuration struct {
	AccessKey     string `yaml:"access_key" json:"access_key"`
	SecretKey     string `yaml:"secret_key" json:"secret_key"`
	Region        string `yaml:"region" json:"region"`
	Endpoint      string `yaml:"endpoint" json:"endpoint"`
	SkipSSLVerify bool   `yaml:"skipSSLverify" json:"skipSSLverify"`
}

type ReportConfiguration struct {
	Format   string           `yaml:"format" json:"format"` // md, csv or html
	Bucket   string           `yaml:"bucket" json:"bucket"` // report will upload to s3 bucket to persist
	S3Config *S3Configuration `yaml:"s3_config" json:"s3_config"`
}

// BenchResult is the struct that will contain the benchmark results from a
// worker after it has finished its benchmark
type BenchmarkResult struct {
	TestName             string
	SuccessfulOperations float64
	FailedOperations     float64
	Workers              float64
	ParallelClients      float64
	Bytes                float64
	// BandwidthAvg is the amount of Bytes per second of runtime
	BandwidthAvg       float64
	LatencyAvg         float64
	GenBytesLatencyAvg float64
	Duration           time.Duration
	// Type               OpType
	FileSize  uint64
	Depth     uint64
	Width     uint64
	BlockSize uint64
}

// CheckSetConfig checks the global config
func CheckSetConfig(config *MigrationConf) {
	if config.GlobalConfig == nil { // TODO: check more configs
		log.WithError(fmt.Errorf("data-migrate global configs need to be set")).Fatalf("Issue detected when scanning through the data-migrate global configs")
	}
	if err := checkMigrationConfig(config); err != nil {
		log.WithError(err).Fatalf("Issue detected when scanning through the config file")
	}
}

func checkMigrationConfig(config *MigrationConf) error {
	if config.GlobalConfig.SourceFsType == "" || config.GlobalConfig.TargetFsType == "" {
		return fmt.Errorf("source and dest fs type need to be set")
	}
	if config.GlobalConfig.TasksFile == "" {
		return fmt.Errorf("data-migrate tasks file need to be set")
	}
	return nil
}

var ReadFile = os.ReadFile

func LoadConfigFromFile(configFile string) *MigrationConf {
	configFileContent, err := ReadFile(configFile)
	if err != nil {
		log.WithError(err).Fatalf("Error reading config file: %s", configFile)
	}
	var config MigrationConf

	if strings.HasSuffix(configFile, ".yaml") || strings.HasSuffix(configFile, ".yml") {
		err = yaml.Unmarshal(configFileContent, &config)
		if err != nil {
			log.WithError(err).Fatalf("Error unmarshaling yaml config file: %s", configFile)
		}
	} else if strings.HasSuffix(configFile, ".json") {
		err = json.Unmarshal(configFileContent, &config)
		if err != nil {
			log.WithError(err).Fatalf("Error unmarshaling json config file: %s", configFile)
		}
	} else {
		log.WithError(err).Fatalf("Configuration file must be a yaml or json formatted file")
	}

	return &config
}

// MigrationTask struct
type MigrationTask struct {
	ID               int    `json:"id"`                 // task id
	SourceDir        string `json:"source_dir"`         // Source directory
	TargetDir        string `json:"target_dir"`         // Target directory
	FileListPath     string `json:"file_list_path"`     // File list path
	SourceFsType     string `json:"source_fs_type"`     // gpfs
	TargetFsType     string `json:"target_fs_type"`     // yrfs_ec
	CheckSourceEntry bool   `json:"check_source_entry"` // Check if the source exists and whether it is a file or directory

	Bucket   string           `json:"bucket"` // logs will upload to s3 bucket to persist
	S3Config *S3Configuration `json:"s3_config"`

	RcloneFlags *RcloneFlags `json:"rclone_flags"`

	Timestamp int64 `json:"timestamp"` // the timestamp of creating server
}

// Parse file list and read all file paths to migrate
func parseFileList(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file list: %w", err)
	}
	defer file.Close()

	var files []string
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			files = append(files, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read file list: %w", err)
	}

	return files, nil
}

// Parse task file
func ParseTaskFile(configPath string) ([]*MigrationTask, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file %s: %s", configPath, err)
	}
	defer file.Close()

	var tasks []*MigrationTask
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lineNum++

		// Split into three columns
		parts := strings.Fields(line)
		if len(parts) > 3 || len(parts) < 2 {
			return nil, fmt.Errorf("line %d: incorrect format, expected 2 or 3 columns, got %d: %s",
				lineNum, len(parts), line)
		}

		// Create migration task
		task := &MigrationTask{
			SourceDir: strings.TrimSpace(parts[0]),
			TargetDir: strings.TrimSpace(parts[1]),
			ID:        lineNum,
		}
		if len(parts) == 3 { // if column 3 not exists, all the source dirs will be copy.
			task.FileListPath = strings.TrimSpace(parts[2])
		}

		tasks = append(tasks, task)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return tasks, nil
}

type MigrationConf struct {
	ReportConfig *ReportConfiguration `yaml:"report_config" json:"report_config"`
	GlobalConfig *GlobalConfiguration `yaml:"global_config" json:"global_config"`
}

type GlobalConfiguration struct {
	SourceFsType string       `yaml:"source_fs_type" json:"source_fs_type"` // cpfs
	TargetFsType string       `yaml:"target_fs_type" json:"target_fs_type"` // yrfs_ec
	TasksFile    string       `yaml:"tasks_file" json:"tasks_file"`         // eg: deploy/data_sources.txt
	RcloneFlags  *RcloneFlags `yaml:"rclone_flags" json:"rclone_flags"`
	// CheckSourceEntry bool         `yaml:"check_source_entry" json:"check_source_entry"` // Check if the source exists and whether it is a file or directory
}
