#!/bin/bash

# Define variables
SERVER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-capacity-statistics-server"
WORKER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-capacity-statistics-worker"
CONFIG_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-capacity-config.yaml"

# 1. Download server and worker binaries
echo "Downloading data-capacity files..."
wget -q "$SERVER_URL" -O data-capacity-statistics-server
wget -q "$WORKER_URL" -O data-capacity-statistics-worker

# Verify download success
if [ ! -f "data-capacity-statistics-server" ] || [ ! -f "data-capacity-statistics-worker" ]; then
    echo "Error: Failed to download required files"
    exit 1
fi

# Make binaries executable
chmod +x data-capacity-statistics-server data-capacity-statistics-worker

# 2. Check if data-capacity-config.yaml exists
if [ ! -f "data-capacity-config.yaml" ]; then
    echo "data-capacity-config.yaml not found, downloading..."
    wget -q "$CONFIG_URL" -O data-capacity-config.yaml
    if [ $? -ne 0 ]; then
        echo "Error: Failed to download data-capacity-config.yaml"
        exit 1
    fi
    echo "data-capacity-config.yaml downloaded successfully. Please check and modify it if needed, then run this script again."
    exit 0
else
    echo "Using local data-capacity-config.yaml"
fi
