package common

import (
	"os"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestConfigFileSuite(t *testing.T) {
	suite.Run(t, new(configFileTestSuite))
}

type configFileTestSuite struct {
	suite.Suite
}

func (s *configFileTestSuite) Test_loadConfigFromYAMLFile() {
	read := func(content []byte) func(string) ([]byte, error) {
		return func(string) ([]byte, error) {
			return content, nil
		}
	}
	defer func() {
		ReadFile = os.ReadFile
	}()
	type args struct {
		configFileContent []byte
	}
	tests := []struct {
		name string
		args args
		want *MigrationConf
	}{
		{"empty file", args{[]byte{}}, &MigrationConf{}},
		{"fs configs", args{[]byte(`report_config:
  format: csv # csv, md or html
  s3_config:
    access_key: secretKey
    secret_key: secretSecret
    region: us-east-1
    endpoint: http://10.9.8.72:80
    bucket: test
    skipSSLverify: true
global_config:
  ultra_large_scale: true # indicate ultra-large-scale task
  server_side_listing: true # Whether to list files on server side
  feishu_url: "feishu"
  fs_types:
    - yrfs
    - gpfs
  tasks_file: deploy/data_sources.txt
  file_list_dir: /mnt/yrfs/public-data/user/zhengliang/
  file_list_dir_fs_type: yrfs_ec
  max_files_per_output: 500000
  concurrency: 3
  #
  # --checkers 128 --transfers 128 --size-only --local-no-set-modtime --log-level INFO --log-file
  rclone_flags:
    checkers: 128
    log_level: INFO
`)}, &MigrationConf{
			ReportConfig: &ReportConfiguration{
				Format: "csv",
				S3Config: &S3Configuration{
					Endpoint:      "http://10.9.8.72:80",
					Bucket:        "test",
					AccessKey:     "secretKey",
					SecretKey:     "secretSecret",
					Region:        "us-east-1",
					SkipSSLVerify: true,
				},
			},
			GlobalConfig: &GlobalConfiguration{
				UltraLargeScale:   true,
				ServerSideListing: true,
				FeishuURL:         "feishu",
				FsTypes:           []string{"yrfs", "gpfs"},
				TasksFile:         "deploy/data_sources.txt",
				FileListDir:       "/mnt/yrfs/public-data/user/zhengliang/",
				FileListDirFsType: "yrfs_ec",
				MaxFilesPerOutput: 500000,
				Concurrency:       3,
				RcloneFlags: &RcloneFlags{
					Checkers: 128,
					LogLevel: "INFO",
				},
			},
		}},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			ReadFile = read(tt.args.configFileContent)
			if got := LoadConfigFromFile("configFile.yaml"); !s.EqualValues(tt.want, got) {
				t.Errorf("loadConfigFromFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (s *configFileTestSuite) Test_loadConfigFromJSONFile() {
	read := func(content []byte) func(string) ([]byte, error) {
		return func(string) ([]byte, error) {
			return content, nil
		}
	}
	defer func() {
		ReadFile = os.ReadFile
	}()
	type args struct {
		configFileContent []byte
	}
	tests := []struct {
		name string
		args args
		want *MigrationConf
	}{
		{"empty file", args{[]byte(`{}`)}, &MigrationConf{}},
		{"fs configs", args{[]byte(`{
"report_config": {
  "format": "csv",
  "s3_config": {
    "access_key": "secretKey",
    "secret_key": "secretSecret",
    "region": "us-east-1",
    "endpoint": "http://10.9.8.72:80",
    "bucket": "test",
    "skipSSLverify": true
  }
},
"global_config": {
  "ultra_large_scale": true,
  "server_side_listing": true,
  "feishu_url": "feishu",
  "fs_types": ["yrfs", "gpfs"],
  "tasks_file": "deploy/data_sources.txt",
  "file_list_dir": "/mnt/yrfs/public-data/user/zhengliang/",
  "file_list_dir_fs_type": "yrfs_ec",
  "max_files_per_output": 500000,
  "concurrency": 3,
  "rclone_flags":  {
    "checkers": 128,
	"log_level": "INFO"
  }
}
}`)}, &MigrationConf{
			ReportConfig: &ReportConfiguration{
				Format: "csv",
				S3Config: &S3Configuration{
					Endpoint:      "http://10.9.8.72:80",
					Bucket:        "test",
					AccessKey:     "secretKey",
					SecretKey:     "secretSecret",
					Region:        "us-east-1",
					SkipSSLVerify: true,
				},
			},
			GlobalConfig: &GlobalConfiguration{
				UltraLargeScale:   true,
				ServerSideListing: true,
				FeishuURL:         "feishu",
				FsTypes:           []string{"yrfs", "gpfs"},
				TasksFile:         "deploy/data_sources.txt",
				FileListDir:       "/mnt/yrfs/public-data/user/zhengliang/",
				FileListDirFsType: "yrfs_ec",
				MaxFilesPerOutput: 500000,
				Concurrency:       3,
				RcloneFlags: &RcloneFlags{
					Checkers: 128,
					LogLevel: "INFO",
				},
			},
		}},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			ReadFile = read(tt.args.configFileContent)
			if got := LoadConfigFromFile("configFile.json"); !s.EqualValues(tt.want, got) {
				t.Errorf("loadConfigFromFile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func (s *configFileTestSuite) TestParseRcloneSize() {
	tests := []struct {
		name  string
		input string
		want  *RcloneSize
	}{
		{"empty dir", `Total objects: 0
Total size: 0 B (0 Byte)`,
			&RcloneSize{
				Objects: 0,
				Size:    "0 B",
				Bytes:   0,
			}},
		{"normal dir", `Total objects: 231
Total size: 696.459 MiB (730290222 Byte)`,
			&RcloneSize{
				Objects: 231,
				Size:    "696.459 MiB",
				Bytes:   730290222,
			}},
		{"large dir", `Total objects: 1k (1000)
Total size: 20.381 MiB (21370754 Byte)`,
			&RcloneSize{
				Objects: 1000,
				Size:    "20.381 MiB",
				Bytes:   21370754,
			}},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			if got, _ := ParseRcloneSize(tt.input); !s.EqualValues(tt.want, got) {
				t.Errorf("ParseRcloneSize() = %v, want %v", got, tt.want)
			}
		})
	}
}
