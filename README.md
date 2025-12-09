# Data Migrate

A distributed data migration system built in Go that uses [rclone](https://github.com/rclone/rclone) for efficient data transfer across multiple clients.

## Overview

This system provides a distributed architecture for migrating large volumes of data using [rclone](https://github.com/rclone/rclone). It consists of a single server that coordinates migration tasks and multiple clients that execute the actual data transfer operations.

## Features

- **Distributed Task Processing**: Multiple clients can work simultaneously on different migration tasks
- **Progress Monitoring**: Real-time progress tracking for all migration tasks
- **Result Persistence**: Task results and rclone logs are saved to S3 for later analysis
- **Resource Management**: Efficient handling of connections and goroutines
- **File System Detection**: Built-in utilities for detecting file system types

## Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │     Server      │    │   Client #1     │
│   (tasks.txt)   │────▶  (Coordinator)  │────▶   (Worker)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                |                  │
                                |                  ▼
                                |                ┌─────────────────┐
                                |                │   rclone copy   │
                                |                │   Execution     │
                                |                └─────────────────┘
                                |                         │
                        ┌─────────────────┐     ┌─────────┴─────────┐
                        │   Task Results  │◀─── │   Source Data     │
                        │                 │     │  Destination      │
                        └─────────────────┘     └───────────────────┘
```

# Installation
## Prerequisites
- [rclone](https://github.com/rclone/rclone) installed and configured on all client machines
- Network connectivity between server and clients

## Building from Source
```bash
# Clone the repository
git clone https://github.com/microyahoo/data-migrate.git
cd data-migrate

make build-local

# build image
make build
```

# Quick Start
## Prepare Configuration File
```yaml
report_config:
  format: csv # csv, md or html
  bucket: test
  s3_config:
    access_key: <access key>
    secret_key: <secret key>
    region: us-east-1
    endpoint: <s3 endpoint>
    skipSSLverify: true

global_config:
  feishu_url: "feishu_url"
  source_fs_type: gpfs
  target_fs_type: yrfs_ec
  tasks_file: deploy/data_sources.txt

  file_list_dir: /mnt/yrfs/public-data/user/zhengliang/
  file_list_dir_fs_type: yrfs_ec
  max_files_per_output: 500000
  concurrency: 3
  rclone_flags:
    checkers: 128
    transfers: 128
    log_level: INFO
    local_no_set_modtime: true
    size_only: true
    dryrun: true
```

## Prepare data sources
```
# Format: source_path destination_path file_list_path
/mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/ /mnt/yrfs/public-data/training/samples/camera_1/ /mnt/core-data/data/3d_object_gt/data_sync/results/sync_3d/mnt#csi-data-gfs#lidar#deeproute_all#samples#camera_1#.txt
/mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2/ /mnt/yrfs/public-data/training/samples/camera_2/ /mnt/core-data/data/3d_object_gt/data_sync/results/sync_3d/mnt#csi-data-gfs#lidar#deeproute_all#samples#camera_2#.txt
/mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3/ /mnt/yrfs/public-data/training/samples/camera_3/
```

## Start the Server
```bash
# Start server with default settings
./bin/data-migrate-server --config.file <config-file>

# Or specify custom port
./bin/data-migrate-server --config.file <config-file> --server.port 2000
```

## Start Clients
On client machines (or multiple terminals on the same machine):
```bash
./bin/data-migrate-worker --server.address <server-ip:port>
```

## Results
| id | source dir | target dir | client id | duration | success | split pattern | split files | log file | message |
|----|------------|------------|-----------|----------|---------|---------------|-------------|----------|---------|
| 1 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/ | /mnt/yrfs/public-data/training/samples/camera_1/ | 127.0.0.1:41170 | 879.66s | false |  | 0 |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1: no such file or directory |
| 2 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2/ | /mnt/yrfs/public-data/training/samples/camera_2/ | 127.0.0.1:41170 | 323.36s | false |  | 0 |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2: no such file or directory |
| 3 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3/ | /mnt/yrfs/public-data/training/samples/camera_3/ | 127.0.0.1:41170 | 362.933s | false |  | 0 |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3: no such file or directory |
| 4 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:41170 | 609.270302ms | true | /mnt/yrfs/public-data/user/zhengliang/1765264052219/k1-32303/test1.txt_*.index | 3 | rclone/log-files/1765264052219/rclone_copy_1765264052219_4_worker2_test1.txt_0003.index.log | Migrated task 4 successfully |
| 5 | /var | /tmp/zhengliang/var | 127.0.0.1:41170 | 249.514503ms | true | /mnt/yrfs/public-data/user/zhengliang/1765264052219/k1-32303/test2.txt_*.index | 1 | rclone/log-files/1765264052219/rclone_copy_1765264052219_5_worker0_test2.txt_0001.index.log | Migrated task 5 successfully |
| 6 | /etc/udev | /tmp/zhengliang/etc/udev | 127.0.0.1:41170 | 299.82239ms | true |  | 0 | rclone/log-files/1765264052219/rclone_copy_1765264052219_6.log | Migrated task 6 successfully |
