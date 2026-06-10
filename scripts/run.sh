#!/bin/bash

# Source directory holding the worker binary (shared storage)
SRC_DIR="/training-vepfs-new/zhengliang"
# Common worker directory on each host and the server address
WORKER_DIR="/root/zhengliang/data-migrate/worker"
SERVER_ADDRESS="10.235.4.156:2000"

ansible all -i hosts -f 64 -m shell -a "mkdir -p ${WORKER_DIR}; cp ${SRC_DIR}/data-delete-worker ${WORKER_DIR};"
ansible all -i hosts -f 64 -m shell -a "nohup ${WORKER_DIR}/data-delete-worker --server.address ${SERVER_ADDRESS} > ${WORKER_DIR}/worker.log 2>&1 &"
