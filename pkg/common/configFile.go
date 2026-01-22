package common

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/goccy/go-yaml"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
)

type MigrationConf struct {
	ReportConfig *ReportConfiguration `yaml:"report_config" json:"report_config"`
	GlobalConfig *GlobalConfiguration `yaml:"global_config" json:"global_config"`
}

type GlobalConfiguration struct {
	FeishuURL         string       `yaml:"feishu_url" json:"feishu_url"`
	FsTypes           []string     `yaml:"fs_types" json:"fs_types"`     // cpfs, yrfs
	TasksFile         string       `yaml:"tasks_file" json:"tasks_file"` // eg: deploy/data_sources.txt
	RcloneFlags       *RcloneFlags `yaml:"rclone_flags" json:"rclone_flags"`
	FileListDir       string       `yaml:"file_list_dir" json:"file_list_dir"`
	FileListDirFsType string       `yaml:"file_list_dir_fs_type" json:"file_list_dir_fs_type"`
	MaxFilesPerOutput int          `yaml:"max_files_per_output" json:"max_files_per_output"`
	Concurrency       int          `yaml:"concurrency" json:"concurrency"`

	UltraLargeScale   bool `yaml:"ultra_large_scale" json:"ultra_large_scale"`
	ServerSideListing bool `yaml:"server_side_listing" json:"server_side_listing"`
}

type RcloneFlags struct {
	Checkers int    `yaml:"checkers" json:"checkers"`
	LogLevel string `yaml:"log_level" json:"log_level"`
}

// S3Configuration contains all information to connect to a certain S3 endpoint
type S3Configuration struct {
	AccessKey     string `yaml:"access_key" json:"access_key"`
	SecretKey     string `yaml:"secret_key" json:"secret_key"`
	Region        string `yaml:"region" json:"region"`
	Endpoint      string `yaml:"endpoint" json:"endpoint"`
	Bucket        string `yaml:"bucket" json:"bucket"` // report will upload to s3 bucket to persist
	SkipSSLVerify bool   `yaml:"skipSSLverify" json:"skipSSLverify"`
}

type ReportConfiguration struct {
	Format   string           `yaml:"format" json:"format"` // md, csv or html
	S3Config *S3Configuration `yaml:"s3_config" json:"s3_config"`
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
	if len(config.GlobalConfig.FsTypes) == 0 {
		return fmt.Errorf("fs type need to be set")
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
	ID           int      `json:"id"`             // task id
	SourceDir    string   `json:"source_dir"`     // Source directory
	FileListPath string   `json:"file_list_path"` // File list path
	FsTypes      []string `json:"fs_types"`       // gpfs, yrfs

	FileListDir       string `json:"file_list_dir"`
	FileListDirFsType string `json:"file_list_dir_fs_type"`
	MaxFilesPerOutput int    `json:"max_files_per_output"`
	Concurrency       int    `json:"concurrency"`

	S3Config *S3Configuration `json:"s3_config"` // logs will upload to s3 bucket to persist

	RcloneFlags *RcloneFlags `json:"rclone_flags"`

	Timestamp int64 `json:"timestamp"` // the timestamp of creating server

	// subtask
	SubTaskID int    `json:"sub_task_id"`
	FileFrom  string `json:"file_from"`
}

func (t *MigrationTask) Check() error {
	if t.SourceDir == "" {
		return fmt.Errorf("Source is empty")
	}
	st, err := GetFilesystemType(t.SourceDir)
	if err != nil {
		return err
	}
	stypeSet := sets.NewString(t.FsTypes...)
	if !stypeSet.Has(st) {
		return fmt.Errorf("filesystem type not match(%s, %s)", st, t.FsTypes)
	}
	if t.FileListPath != "" {
		ft, err := GetFilesystemType(t.FileListDir)
		if err != nil {
			return err
		}
		if ft != t.FileListDirFsType {
			return fmt.Errorf("actual file list directory filesystem type not match(%s, %s)", ft, t.FileListDirFsType)
		}
	}
	si, err := os.Stat(t.SourceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("source dir %s not exists", t.SourceDir)
		} else {
			return fmt.Errorf("failed to stat source dir %s: %s", t.SourceDir, err)
		}
	} else if !si.IsDir() {
		return fmt.Errorf("source dir %s is not directory", t.SourceDir)
	}
	if t.FileFrom != "" {
		ff, err := os.Stat(t.FileFrom)
		if err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("file from %s not exists", t.FileFrom)
			} else {
				return fmt.Errorf("failed to stat file from %s: %s", t.FileFrom, err)
			}
		} else if ff.IsDir() {
			return fmt.Errorf("file from %s is a directory", t.SourceDir)
		}
	}
	if t.FileListPath == "" {
		// not specify file list path
		return nil
	}
	fi, err := os.Stat(t.FileListPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("file list path %s not exists", t.FileListPath)
		} else {
			return fmt.Errorf("failed to stat file list path %s: %s", t.FileListPath, err)
		}
	} else if fi.IsDir() {
		return fmt.Errorf("file list path %s is directory", t.FileListPath)
	}
	return nil
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

func (t *MigrationTask) DeepCopyJSON() (*MigrationTask, error) {
	bytes, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	var dst MigrationTask
	err = json.Unmarshal(bytes, &dst)
	if err != nil {
		return nil, err
	}
	return &dst, nil
}

// Parse task file
func ParseTaskFile(conf *MigrationConf) ([]*MigrationTask, error) {
	var (
		globalConfig = conf.GlobalConfig
		reportConfig = conf.ReportConfig
		configPath   = globalConfig.TasksFile
		timestamp    = time.Now().Unix()
	)

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
		if len(parts) > 2 {
			return nil, fmt.Errorf("line %d: incorrect format, expected 1 column, got %d: %s",
				lineNum, len(parts), line)
		}

		// Create migration task
		task := &MigrationTask{
			SourceDir:   strings.TrimSpace(parts[0]),
			ID:          lineNum,
			FsTypes:     globalConfig.FsTypes,
			RcloneFlags: globalConfig.RcloneFlags,
			S3Config:    reportConfig.S3Config,
			Timestamp:   timestamp,

			FileListDir:       globalConfig.FileListDir,
			FileListDirFsType: globalConfig.FileListDirFsType,
			MaxFilesPerOutput: globalConfig.MaxFilesPerOutput,
			Concurrency:       globalConfig.Concurrency,
		}
		if len(parts) == 2 {
			task.FileListPath = strings.TrimSpace(parts[1])
		}

		tasks = append(tasks, task)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	return tasks, nil
}

// RcloneSize parse the result of 'rclone size'
type RcloneSize struct {
	Objects int64  // objects
	Size    string // size, eg. 10 GB
	Bytes   int64  // bytes
}

func ParseRcloneSize(output string) (*RcloneSize, error) {
	// Total objects: 231
	// Total size: 695.903 MiB (729707029 Byte)
	var (
		objects int64
		size    string
		bytes   int64
	)
	// - Total objects: 348
	// - Total objects: 1k (1000)
	// - Total objects: 2.5M (2500000)
	// objRegex := regexp.MustCompile(`Total objects:\s*(\d+)`)
	objRegex := regexp.MustCompile(`Total objects:\s*(?:[\d\.]+[kMGTPE]?\s*\()?(\d+)\)?`)
	objMatch := objRegex.FindStringSubmatch(output)
	if len(objMatch) >= 2 {
		fmt.Sscanf(objMatch[1], "%d", &objects)
	}

	// match size and bytes
	sizeRegex := regexp.MustCompile(`Total size:\s*([\d\.]+\s+\w+)\s+\((\d+)\s+Bytes?\)`)
	sizeMatch := sizeRegex.FindStringSubmatch(output)
	if len(sizeMatch) > 2 {
		size = sizeMatch[1]                    // 44.882 GiB
		fmt.Sscanf(sizeMatch[2], "%d", &bytes) // 48191611197
	}

	if size == "" {
		return nil, fmt.Errorf("failed to parse rclone size")
	}

	return &RcloneSize{Objects: objects, Size: size, Bytes: bytes}, nil
}
