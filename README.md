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
  ultra_large_scale: false   # indicate ultra-large-scale task
  server_side_listing: false # Whether to list files on server side

  feishu_url: "feishu_url"
  source_fs_types:
    - yrfs_ec
    - gpfs
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
| id | subtask id | source dir | target dir | client id | duration | success | split pattern | split files | file from | log file | message |
|----|------------|------------|------------|-----------|----------|---------|---------------|-------------|-----------|----------|---------|
| 1 | 0 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/ | /mnt/yrfs/public-data/training/samples/camera_1/ | 127.0.0.1:44476 | 827.565s | false |  | 0 |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1: no such file or directory |
| 2 | 0 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2/ | /mnt/yrfs/public-data/training/samples/camera_2/ | 127.0.0.1:44476 | 213.905s | false |  | 0 |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2: no such file or directory |
| 3 | 0 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3/ | /mnt/yrfs/public-data/training/samples/camera_3/ | 127.0.0.1:44476 | 262.946s | false |  | 0 |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3: no such file or directory |
| 4 | 0 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:44476 | 245.162616ms | true | /mnt/yrfs/public-data/user/zhengliang/1765869105/k1-124103/test1.txt_*.index | 3 |  | rclone/log-files/1765869105/rclone_copy_1765869105_4_worker2_test1.txt_0003.index.log | Migrated task 4 successfully |
| 5 | 0 | /var | /tmp/zhengliang/var | 127.0.0.1:44476 | 211.516957ms | true | /mnt/yrfs/public-data/user/zhengliang/1765869105/k1-124103/test2.txt_*.index | 1 |  | rclone/log-files/1765869105/rclone_copy_1765869105_5_worker0_test2.txt_0001.index.log | Migrated task 5 successfully |
| 6 | 0 | /etc/udev | /tmp/zhengliang/etc/udev | 127.0.0.1:44476 | 153.251914ms | true | /mnt/yrfs/public-data/user/zhengliang/1765869105/k1-124103/udev_*.index | 1 |  | rclone/log-files/1765869105/rclone_copy_1765869105_6_worker0_udev_0001.index.log | Migrated task 6 successfully |
| 7 | 0 | /root/go/src/github.com/rclone | /tmp/zhengliang/rclone | 127.0.0.1:44476 | 269.019774ms | true | /mnt/yrfs/public-data/user/zhengliang/1765869105/k1-124103/rclone_*.index | 3 |  | rclone/log-files/1765869105/rclone_copy_1765869105_7_worker2_rclone_0001.index.log | Migrated task 7 successfully |

| id | subtask id | source dir | target dir | client id | duration | success | split pattern | split files | file from | log file | message |
|----|------------|------------|------------|-----------|----------|---------|---------------|-------------|-----------|----------|---------|
| 1 | 1 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:44426 | 580.579634ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/test1.txt_0001.index | rclone/log-files/1765868733/rclone_copy_1765868733_1_worker0_test1.txt_0001.index.log | Migrated task 1 with subtask 1 successfully |
| 1 | 2 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:44426 | 684.392755ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/test1.txt_0002.index | rclone/log-files/1765868733/rclone_copy_1765868733_1_worker0_test1.txt_0002.index.log | Migrated task 1 with subtask 2 successfully |
| 1 | 3 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:44426 | 408.588032ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/test1.txt_0003.index | rclone/log-files/1765868733/rclone_copy_1765868733_1_worker0_test1.txt_0003.index.log | Migrated task 1 with subtask 3 successfully |
| 1 | 0 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang |  | 1.673749696s | true |  | 0 |  |  |  |
| 2 | 1 | /var | /tmp/zhengliang/var | 127.0.0.1:44426 | 211.162227ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/test2.txt_0001.index | rclone/log-files/1765868733/rclone_copy_1765868733_2_worker0_test2.txt_0001.index.log | Migrated task 2 with subtask 1 successfully |
| 2 | 0 | /var | /tmp/zhengliang/var |  | 211.161139ms | true |  | 0 |  |  |  |
| 3 | 1 | /etc/udev | /tmp/zhengliang/etc/udev | 127.0.0.1:44426 | 202.684248ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/udev_0001.index | rclone/log-files/1765868733/rclone_copy_1765868733_3_worker0_udev_0001.index.log | Migrated task 3 with subtask 1 successfully |
| 3 | 0 | /etc/udev | /tmp/zhengliang/etc/udev |  | 202.682738ms | true |  | 0 |  |  |  |
| 4 | 1 | /root/go/src/github.com/rclone | /tmp/zhengliang/rclone | 127.0.0.1:44426 | 455.557088ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/rclone_0001.index | rclone/log-files/1765868733/rclone_copy_1765868733_4_worker0_rclone_0001.index.log | Migrated task 4 with subtask 1 successfully |
| 4 | 2 | /root/go/src/github.com/rclone | /tmp/zhengliang/rclone | 127.0.0.1:44426 | 854.990683ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/rclone_0002.index | rclone/log-files/1765868733/rclone_copy_1765868733_4_worker0_rclone_0002.index.log | Migrated task 4 with subtask 2 successfully |
| 4 | 3 | /root/go/src/github.com/rclone | /tmp/zhengliang/rclone | 127.0.0.1:44426 | 811.397944ms | true |  | 0 | /mnt/yrfs/public-data/user/zhengliang/1765868733/k1-122346/rclone_0003.index | rclone/log-files/1765868733/rclone_copy_1765868733_4_worker0_rclone_0003.index.log | Migrated task 4 with subtask 3 successfully |
| 4 | 0 | /root/go/src/github.com/rclone | /tmp/zhengliang/rclone |  | 2.122083391s | true |  | 0 |  |  |  |
