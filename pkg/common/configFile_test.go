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
  bucket: test
  s3_config:
    access_key: secretKey
    secret_key: secretSecret
    region: us-east-1
    endpoint: http://10.9.8.72:80
    skipSSLverify: true
global_config:
  ultra_large_scale: true # indicate ultra-large-scale task
  server_side_listing: true # Whether to list files on server side
  feishu_url: "feishu"
  source_fs_types:
    - yrfs
    - gpfs
  target_fs_types:
    - yrfs_ec
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
    transfers: 64
    log_level: INFO
    local_no_set_modtime: true
    size_only: true
    dryrun: true
`)}, &MigrationConf{
			ReportConfig: &ReportConfiguration{
				Format: "csv",
				Bucket: "test",
				S3Config: &S3Configuration{
					Endpoint:      "http://10.9.8.72:80",
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
				SourceFsTypes:     []string{"yrfs", "gpfs"},
				TargetFsTypes:     []string{"yrfs_ec", "gpfs"},
				TasksFile:         "deploy/data_sources.txt",
				FileListDir:       "/mnt/yrfs/public-data/user/zhengliang/",
				FileListDirFsType: "yrfs_ec",
				MaxFilesPerOutput: 500000,
				Concurrency:       3,
				RcloneFlags: &RcloneFlags{
					Checkers:          128,
					Transfers:         64,
					LogLevel:          "INFO",
					LocalNoSetModtime: true,
					SizeOnly:          true,
					Dryrun:            true,
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
  "bucket": "test",
  "s3_config": {
    "access_key": "secretKey",
    "secret_key": "secretSecret",
    "region": "us-east-1",
    "endpoint": "http://10.9.8.72:80",
    "skipSSLverify": true
  }
},
"global_config": {
  "ultra_large_scale": true,
  "server_side_listing": true,
  "feishu_url": "feishu",
  "source_fs_types": ["yrfs", "gpfs"],
  "target_fs_types": ["yrfs_ec", "gpfs"],
  "tasks_file": "deploy/data_sources.txt",
  "file_list_dir": "/mnt/yrfs/public-data/user/zhengliang/",
  "file_list_dir_fs_type": "yrfs_ec",
  "max_files_per_output": 500000,
  "concurrency": 3,
  "rclone_flags":  {
    "checkers": 128,
	"transfers": 64,
	"log_level": "INFO",
	"local_no_set_modtime": true,
    "size_only": true,
	"dryrun": true
  }
}
}`)}, &MigrationConf{
			ReportConfig: &ReportConfiguration{
				Format: "csv",
				Bucket: "test",
				S3Config: &S3Configuration{
					Endpoint:      "http://10.9.8.72:80",
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
				SourceFsTypes:     []string{"yrfs", "gpfs"},
				TargetFsTypes:     []string{"yrfs_ec", "gpfs"},
				TasksFile:         "deploy/data_sources.txt",
				FileListDir:       "/mnt/yrfs/public-data/user/zhengliang/",
				FileListDirFsType: "yrfs_ec",
				MaxFilesPerOutput: 500000,
				Concurrency:       3,
				RcloneFlags: &RcloneFlags{
					Checkers:          128,
					Transfers:         64,
					LogLevel:          "INFO",
					LocalNoSetModtime: true,
					SizeOnly:          true,
					Dryrun:            true,
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
