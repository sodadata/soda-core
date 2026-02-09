#!/usr/bin/env bash

set -e
set -x

cd ..
rm -rf soda_cli
mkdir soda_cli
cd soda_cli

uv venv .venv
uv pip install --index-url https://pypi.dev.sodadata.io/ soda-core soda-postgres
