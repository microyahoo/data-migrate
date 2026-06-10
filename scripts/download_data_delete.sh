#!/bin/bash

# Define variables
SERVER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-delete-server"
WORKER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-delete-worker"
CONFIG_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-delete-config.yaml"

# 1. Download server and worker binaries
echo "Downloading data-delete files..."
wget -q "$SERVER_URL" -O data-delete-server
wget -q "$WORKER_URL" -O data-delete-worker

# Verify download success
if [ ! -f "data-delete-server" ] || [ ! -f "data-delete-worker" ]; then
    echo "Error: Failed to download required files"
    exit 1
fi

# Make binaries executable
chmod +x data-delete-server data-delete-worker

# 2. Check if data-delete-config.yaml exists
if [ ! -f "data-delete-config.yaml" ]; then
    echo "data-delete-config.yaml not found, downloading..."
    wget -q "$CONFIG_URL" -O data-delete-config.yaml
    if [ $? -ne 0 ]; then
        echo "Error: Failed to download data-delete-config.yaml"
        exit 1
    fi
    echo "data-delete-config.yaml downloaded successfully. Please check and modify it if needed, then run this script again."
    exit 0
else
    echo "Using local data-delete-config.yaml"
fi
