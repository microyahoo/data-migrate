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
  source_fs_type: gpfs
  target_fs_type: yrfs_ec
  tasks_file: deploy/data_sources.txt # data sources
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
| ID | Source Directory | Target Directory | Client ID | Duration | Success | Include File | Log File | Message |
|----|------------------|------------------|-----------|----------|---------|--------------|----------|---------|
| 1 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1/ | /mnt/yrfs/public-data/training/samples/camera_1/ | 127.0.0.1:35644 | 851.373us | false |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_1: no such file or directory |
| 2 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2/ | /mnt/yrfs/public-data/training/samples/camera_2/ | 127.0.0.1:35644 | 347.086us | false |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_2: no such file or directory |
| 3 | /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3/ | /mnt/yrfs/public-data/training/samples/camera_3/ | 127.0.0.1:35644 | 224.726us | false |  |  | path does not exist: stat /mnt/csi-data-gfs/lidar/deeproute_all/samples/camera_3: no such file or directory |
| 4 | /root/go/src/deeproute/ops-kubeconfig/ | /tmp/zhengliang | 127.0.0.1:35644 | 996.56162ms | true | rclone/include-files/1764920244408/include_4_2571331201.txt | rclone/log-files/1764920244408/rclone_copy_1764920244408_4.txt | Migrated task 4 with include file /tmp/include_4_2571331201.txt successfully |
| 5 | /var | /tmp/zhengliang/var | 127.0.0.1:35644 | 277.877946ms | true | rclone/include-files/1764920244408/include_5_1088298075.txt | rclone/log-files/1764920244408/rclone_copy_1764920244408_5.txt | Migrated task 5 with include file /tmp/include_5_1088298075.txt successfully |
| 6 | /etc/udev | /tmp/zhengliang/etc/udev | 127.0.0.1:35644 | 303.633042ms | true |  | rclone/log-files/1764920244408/rclone_copy_1764920244408_6.txt | Migrated task 6 with include file successfully |
