#!/bin/bash
# This script monitors the streaming job

echo "Monitoring streaming job..."

# Add your monitoring commands here. This could be checking for logs, process status, etc.
ps aux | grep binance_streaming.py

echo "Monitoring completed."
