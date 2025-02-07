#!/usr/bin/env bash
set -x

# Install dependencies
sudo apt-get update && sudo apt-get install libsasl2-dev

# Create the virtual environment if it does not already exist
if [ ! -d .venv ]; then
  scripts/recreate_venv.sh
fi

# Activate the virtual environment
source .venv/bin/activate

# Setup pre-commit
pre-commit install