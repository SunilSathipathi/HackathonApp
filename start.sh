#!/usr/bin/env bash
set -euo pipefail

# Copy env template if missing
if [ ! -f .env ]; then
  cp .env.example .env
fi

# Create venv if missing
if [ ! -d venv ]; then
  python3 -m venv venv
fi

# Activate venv
source venv/bin/activate

# Install deps
pip install -r requirements.txt

# Run app
python main.py