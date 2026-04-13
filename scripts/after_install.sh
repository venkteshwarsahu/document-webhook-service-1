#!/bin/bash

cd /home/ubuntu/document-webhook-service-1

echo "Setting up virtual environment..."

python3 -m venv .venv
source .venv/bin/activate

echo "Installing dependencies..."
pip install -r requirements.txt