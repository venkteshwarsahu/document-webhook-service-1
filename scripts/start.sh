#!/bin/bash

cd /home/ubuntu/document-webhook-service-1

echo "Stopping existing worker..."
pkill -f main.py || true

echo "Starting worker..."
nohup python main.py > output.log 2>&1 &