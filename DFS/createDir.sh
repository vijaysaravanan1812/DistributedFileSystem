#!/bin/bash

# Base directory
BASE_DIR="/tmp/DFS"

# List of subdirectories
DIRS=("Dnode1" "Dnode2" "Client")

# Create directories if they don't exist
for dir in "${DIRS[@]}"; do
    FULL_PATH="$BASE_DIR/$dir"
    if [ -d "$FULL_PATH" ]; then
        echo "Directory already exists: $FULL_PATH"
    else
        mkdir -p "$FULL_PATH"
        echo "Created directory: $FULL_PATH"
    fi
done
