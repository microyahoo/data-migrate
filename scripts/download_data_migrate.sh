#!/bin/bash

# Define variables
SERVER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-migrate-server"
WORKER_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-migrate-worker"
CONFIG_URL="http://s3-smd.deeproute.cn/smd-pkg/tools/data-migrate/data-migrate-config.yaml"

# 1. Download server and worker binaries
echo "Downloading data-migrate files..."
wget -q "$SERVER_URL" -O data-migrate-server
wget -q "$WORKER_URL" -O data-migrate-worker

# Verify download success
if [ ! -f "data-migrate-server" ] || [ ! -f "data-migrate-worker" ]; then
    echo "Error: Failed to download required files"
    exit 1
fi

# Make binaries executable
chmod +x data-migrate-server data-migrate-worker

# 2. Check if data-migrate-config.yaml exists
if [ ! -f "data-migrate-config.yaml" ]; then
    echo "data-migrate-config.yaml not found, downloading..."
    wget -q "$CONFIG_URL" -O data-migrate-config.yaml
    if [ $? -ne 0 ]; then
        echo "Error: Failed to download data-migrate-config.yaml"
        exit 1
    fi
    echo "data-migrate-config.yaml downloaded successfully. Please check and modify it if needed, then run this script again."
    exit 0
else
    echo "Using local data-migrate-config.yaml"
fi


# 3. Start server on the server node
#echo "Starting data-migrate server on $SERVER_IP..."
# nohup ./data-migrate-server --config.file data-migrate-config.yaml > server.log 2>&1 &

# # Wait for server to initialize
# sleep 5

# # 4. Start workers on all nodes (including server node)
# for ip in $(eval echo "$WORKER_IP_RANGE"); do  # Expand {1..10} using eval
#     echo "Starting data-migrate worker on $ip..."
#     if [ "$ip" == "$SERVER_IP" ]; then
#         # Local execution
#         nohup ./data-migrate-worker --server.address "$SERVER_IP:$SERVER_PORT" > "worker-$ip.log" 2>&1 &
#     else
#         # Remote execution via SSH
#         ssh -o ConnectTimeout=30 -f root@"$ip" "nohup bash -c 'wget -q $WORKER_URL -O data-migrate-worker && chmod +x data-migrate-worker && nohup ./data-migrate-worker --server.address $SERVER_IP:$SERVER_PORT > worker-$ip.log 2>&1' &"
#     fi
# done

# echo "All data-migrate workers started."
# echo "Server is running on $SERVER_IP, workers are running on all nodes."
# echo "Server logs: server.log"
# echo "Worker logs: worker-<ip>.log"
