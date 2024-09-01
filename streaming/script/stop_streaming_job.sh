#!/bin/bash
# This script stops the streaming job

echo "Stopping streaming job..."

# Find the process ID (PID) of the Python script and terminate it
pkill -f binance_streaming.py

echo "Streaming job stopped."
