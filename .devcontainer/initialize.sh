#!/usr/bin/env bash
set -x

# Create the virtual environment if it does not already exist
if [ ! -d .venv ]; then
  scripts/recreate_venv.sh
fi

# Activate the virtual environment
source .venv/bin/activate

# Setup pre-commit
pre-commit install