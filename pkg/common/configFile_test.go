package common

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/suite"
)

func TestConfigFileSuite(t *testing.T) {
	suite.Run(t, new(configFileTestSuite))
}

type configFileTestSuite struct {
	suite.Suite
}

func (s *configFileTestSuite) Test_checkTestCase() {
	type args struct {
		testcase *TestConf
	}
	var (
		tbool = true
		fbool = false
	)
	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    *TestConf
	}{
		{"No defined", args{new(TestConf)}, true, nil},
		{"No fsd or fwd defined", args{&TestConf{
			Tests: []*TestCaseConfiguration{
				{Name: "test1"},
				{Name: "test2", FSD: &FSD{
					Depth: 2,
					Width: 3,
				}},
			}}}, true, nil},
		{"check successfully", args{
			&TestConf{
				GlobalConfig: &GlobalConfiguration{
					Anchor:     "/volume",
					DirectIO:   &tbool,
					Threads:    []uint64{2, 4, 6, 8},
					Operations: []string{"write", "read"},
					Workers:    3,
				},
				Tests: []*TestCaseConfiguration{
					{
						Name: "test1",
						FSD: &FSD{
							Depth:    4,
							Width:    5,
							Files:    1000,
							Size:     4096,
							DirectIO: &fbool,
						},
						FWD: &FWD{
							Threads:   []uint64{16, 64},
							BlockSize: 4096,
						},
					},
					{
						Name: "test2",
						FSD: &FSD{
							Depth: 2,
							Width: 3,
							Files: 10,
							Size:  1024,
						},
						FWD: &FWD{
							BlockSize: 4096,
						},
					},
				},
			},
		},
			false,
			&TestConf{
				GlobalConfig: &GlobalConfiguration{
					Anchor:     "/volume",
					DirectIO:   &tbool,
					Threads:    []uint64{2, 4, 6, 8},
					Operations: []string{"write", "read"},
					Workers:    3,
				},
				Tests: []*TestCaseConfiguration{
					{
						Name: "test1",
						FSD: &FSD{
							Anchor:   "/volume",
							Depth:    4,
							Width:    5,
							Files:    1000,
							Size:     4096,
							DirectIO: &fbool,
						},
						FWD: &FWD{
							Operations: []string{"write", "read"},
							Threads:    []uint64{16, 64},
							BlockSize:  4096,
						},
					},
					{
						Name: "test2",
						FSD: &FSD{
							Anchor:   "/volume",
							Depth:    2,
							Width:    3,
							Files:    10,
							Size:     1024,
							DirectIO: &tbool,
						},
						FWD: &FWD{
							Threads:    []uint64{2, 4, 6, 8},
							Operations: []string{"write", "read"},
							BlockSize:  4096,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			if err := checkTestCase(tt.args.testcase); (err != nil) != tt.wantErr {
				t.Errorf("checkTestCase() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil && !s.EqualValues(tt.want, tt.args.testcase) {
				t.Errorf("checkTestCase() want = %v, got = %v", tt.want, tt.args.testcase)
			}
		})
	}
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
	var (
		tbool = true
		fbool = false
	)
	tests := []struct {
		name string
		args args
		want *TestConf
	}{
		{"empty file", args{[]byte{}}, &TestConf{}},
		{"fsd option nil", args{[]byte(`tests:
  - name: 4k
`)}, &TestConf{
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
				},
			},
		}},
		{"direct_io not specified", args{[]byte(`
tests:
  - name: "4k"
    fsd:
      anchor: "/volume"
      depth: 2
      width: 3
      files: 100000
      size: "4k"
`)}, &TestConf{
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
					FSD: &FSD{
						Anchor:   "/volume",
						DirectIO: nil,
						Depth:    2,
						Width:    3,
						Files:    100000,
						Size:     4096,
					},
				},
			},
		}},
		{"fs configs", args{[]byte(`report_config:
  format: csv # csv, md or html
  bucket: test
  s3_config:
    access_key: secretKey
    secret_key: secretSecret
    region: us-east-1
    endpoint: http://10.9.8.72:80
    skipSSLverify: true
client_configs:
  - name: k1
    ip: 10.3.11.81
  - name: k2
    ip: 10.3.11.82
  - name: k3
    ip: 10.3.11.83
global_config:
  reorder_tasks: true # having a different node read/stat back data than wrote it
  anchor: /volume
  direct_io: false
  threads: [4, 8, 16, 32, 64, 128, 256]
  operations: ["write", "read", "stat", "delete"] # NOTE: When performing read/stat/delete operations, ensure that the file exists
  workers: 3
tests:
  - name: 4k
    fsd:
      anchor: /volume1  # override global value
      direct_io: true  # override global value
      depth: 2
      width: 3
      files: 100000
      size: 4k
    fwd:
      operations: ["write", "read", "stat", "delete"] # NOTE: When performing read/stat/delete operations, ensure that the file exists
      block_size: 4k
      threads: [4, 8, 16, 32, 64, 128, 256]   # override global value
  - name: 8k
    fsd:
      anchor: /volume2
      # direct_io: true
      depth: 4
      width: 5
      files: 100
      size: 8k
    fwd:
      # operations: ["write", "read", "stat", "delete"] # NOTE: When performing read/stat/delete operations, ensure that the file exists
      block_size: 8k
`)}, &TestConf{
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
				ReorderTasks: true,
				Anchor:       "/volume",
				DirectIO:     &fbool,
				Threads:      []uint64{4, 8, 16, 32, 64, 128, 256},
				Operations:   []string{"write", "read", "stat", "delete"},
				Workers:      3,
			},
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
					FSD: &FSD{
						Anchor:   "/volume1",
						DirectIO: &tbool,
						Depth:    2,
						Width:    3,
						Files:    100000,
						Size:     4096,
					},
					FWD: &FWD{
						BlockSize:  4096,
						Threads:    []uint64{4, 8, 16, 32, 64, 128, 256},
						Operations: []string{"write", "read", "stat", "delete"},
					},
				},
				{
					Name: "8k",
					FSD: &FSD{
						Anchor:   "/volume2",
						DirectIO: nil,
						Depth:    4,
						Width:    5,
						Files:    100,
						Size:     8192,
					},
					FWD: &FWD{
						BlockSize: 8192,
					},
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
	var (
		tbool = true
		fbool = false
	)
	tests := []struct {
		name string
		args args
		want *TestConf
	}{
		{"empty file", args{[]byte(`{}`)}, &TestConf{}},
		{"fsd option nil", args{[]byte(`{
"tests":
  [
    {
      "name": "4k"
    }
  ]
}`)}, &TestConf{
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
				},
			},
		}},
		{"direct_io not specified", args{[]byte(`{
"tests":
[
  {
    "name": "4k",
	"fsd": {
	  "anchor": "/volume",
	  "depth": 2,
	  "width": 3,
	  "files": 100000,
	  "size": "4k"
	}
  }
]}`)}, &TestConf{
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
					FSD: &FSD{
						Anchor:   "/volume",
						DirectIO: nil,
						Depth:    2,
						Width:    3,
						Files:    100000,
						Size:     4096,
					},
				},
			},
		}},
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
"client_configs":
[
  {
    "name": "k1",
	"ip": "10.3.11.81"
  },
  {
    "name": "k2",
	"ip": "10.3.11.82"
  },
  {
    "name": "k3",
	"ip": "10.3.11.83"
  }
],
"global_config": {
  "reorder_tasks": true,
  "anchor": "/volume",
  "direct_io": false,
  "threads": [4, 8, 16, 32, 64, 128, 256],
  "operations": ["write", "read", "stat", "delete"],
  "workers": 3
},
"tests": [
  {
    "name": "4k",
    "fsd": {
      "anchor": "/volume1",
      "direct_io": true,
      "depth": 2,
      "width": 3,
      "files": 100000,
      "size": "4k"
	},
    "fwd": {
      "operations": ["write", "read", "stat", "delete"],
      "block_size": "4k",
      "threads": [4, 8, 16, 32, 64, 128, 256]
	}
  },
  {
    "name": "8k",
    "fsd": {
      "anchor": "/volume2",
      "depth": 4,
      "width": 5,
      "files": 100,
      "size": "8k"
	},
    "fwd": {
      "block_size": "8k"
	}
  }
]}`)}, &TestConf{
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
				ReorderTasks: true,
				Anchor:       "/volume",
				DirectIO:     &fbool,
				Threads:      []uint64{4, 8, 16, 32, 64, 128, 256},
				Operations:   []string{"write", "read", "stat", "delete"},
				Workers:      3,
			},
			Tests: []*TestCaseConfiguration{
				{
					Name: "4k",
					FSD: &FSD{
						Anchor:   "/volume1",
						DirectIO: &tbool,
						Depth:    2,
						Width:    3,
						Files:    100000,
						Size:     4096,
					},
					FWD: &FWD{
						BlockSize:  4096,
						Threads:    []uint64{4, 8, 16, 32, 64, 128, 256},
						Operations: []string{"write", "read", "stat", "delete"},
					},
				},
				{
					Name: "8k",
					FSD: &FSD{
						Anchor:   "/volume2",
						DirectIO: nil,
						Depth:    4,
						Width:    5,
						Files:    100,
						Size:     8192,
					},
					FWD: &FWD{
						BlockSize: 8192,
					},
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

func (s *configFileTestSuite) Test_marshal_unmarshal_fsd() {
	type args struct {
		fsd *FSD
	}
	tbool := true
	tests := []struct {
		name string
		args args
		want *FSD
	}{
		{
			name: "marshal and unmarshal",
			args: args{
				fsd: &FSD{
					Anchor:   "/volume",
					DirectIO: &tbool,
					Depth:    10,
					Width:    20,
					Files:    30,
					Size:     4096,
				},
			},
			want: &FSD{
				Anchor:   "/volume",
				DirectIO: &tbool,
				Depth:    10,
				Width:    20,
				Files:    30,
				Size:     4096,
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			bytes, err := json.Marshal(tt.args.fsd)
			s.NoError(err)
			s.T().Logf("**bytes: %s", string(bytes))
			var fsd1 *FSD
			err = json.Unmarshal(bytes, &fsd1)
			s.NoError(err)
			s.EqualValues(tt.want, fsd1)

			var fsd2 FSD
			bytes, err = yaml.Marshal(tt.args.fsd)
			s.NoError(err)
			s.T().Logf("**bytes: %s", string(bytes))
			err = yaml.Unmarshal(bytes, &fsd2)
			s.NoError(err)
			s.EqualValues(tt.want, &fsd2)
		})
	}
}

func (s *configFileTestSuite) Test_marshal_unmarshal_fwd() {
	type args struct {
		fwd *FWD
	}
	tests := []struct {
		name string
		args args
		want *FWD
	}{
		{
			name: "marshal and unmarshal",
			args: args{
				fwd: &FWD{
					Threads:    []uint64{4, 8, 16, 32, 64},
					Operations: []string{"write", "read", "stat", "delete"},
					BlockSize:  4096,
				},
			},
			want: &FWD{
				Threads:    []uint64{4, 8, 16, 32, 64},
				Operations: []string{"write", "read", "stat", "delete"},
				BlockSize:  4096,
			},
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			bytes, err := json.Marshal(tt.args.fwd)
			s.NoError(err)
			s.T().Logf("**bytes: %s", string(bytes))
			var fwd1 *FWD
			err = json.Unmarshal(bytes, &fwd1)
			s.NoError(err)
			s.EqualValues(tt.want, fwd1)

			var fwd2 FWD
			bytes, err = yaml.Marshal(tt.args.fwd)
			s.NoError(err)
			s.T().Logf("**bytes: %s", string(bytes))
			err = yaml.Unmarshal(bytes, &fwd2)
			s.NoError(err)
			s.EqualValues(tt.want, &fwd2)
		})
	}
}
