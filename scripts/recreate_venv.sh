#!/usr/bin/env bash

set -e
rm -rf .venv
uv sync --all-packages --group dev
