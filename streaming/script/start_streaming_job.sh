#!/bin/bash
# This script starts the streaming job

echo "Starting streaming job..."

# Set the path to the script
SCRIPT_PATH="/opt/airflow/script/binance_streaming.py"

# Run the Python script
python3 "$SCRIPT_PATH"

echo "Streaming job started."
