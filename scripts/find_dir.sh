#!/bin/bash

# Base directory to scan for first-level subdirectories
BASE_DIR="/mnt/yrfs/public-data/training/"

find "${BASE_DIR}" -maxdepth 1 -mindepth 1 -type d
