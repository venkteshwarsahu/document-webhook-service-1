#!/bin/bash

echo "Stopping existing worker..."

pkill -f main.py || true