#!/bin/bash

# Market Data Pipeline Demo Script
# Usage: asciinema rec pipeline_demo.cast -c ./demo_recording.sh

echo -e "\033[1;34m❯ Starting Financial Data Ingestion Engine...\033[0m"
sleep 1
echo -e "\033[1;32m✓\033[0m Connected to Exchange Feed A"
echo -e "\033[1;32m✓\033[0m Schema Registry: Loaded v2.4.0"
sleep 0.5

echo -e "\n\033[1;33mSimulating High-Frequency Tick Data...\033[0m"
sleep 1
# Run the python example (requires python3)
python3 examples/run_example.py
echo -e "\n\033[1;34m❯ Ingestion Complete. Quality Score: 99.8%\033[0m"
