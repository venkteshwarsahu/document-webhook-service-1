#!/bin/bash

cd /home/ubuntu/document-webhook-service-1

echo "Stopping existing worker..."
pkill -f main.py || true

echo "Starting worker..."

source .venv/bin/activate

nohup python3 main.py > output.log 2>&1 &