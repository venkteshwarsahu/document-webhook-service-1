#!/bin/bash
set -e

APP_DIR="/home/ubuntu/document-webhook-service-1"

echo "Fixing permissions..."
sudo chown -R ubuntu:ubuntu $APP_DIR

cd $APP_DIR

echo "Removing old venv..."
rm -rf .venv

echo "Creating virtual environment..."
python3 -m venv .venv

echo "Activating virtual environment..."
source .venv/bin/activate

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing dependencies..."
pip install -r requirements.txt