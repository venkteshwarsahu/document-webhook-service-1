#!/bin/bash
set -e

APP_DIR="/home/ubuntu/document-webhook-service-1"

cd $APP_DIR

echo "Stopping old process..."
pkill -f main.py || true

echo "Activating venv..."
source .venv/bin/activate

echo "Starting app..."
nohup python3 main.py > output.log 2>&1 &