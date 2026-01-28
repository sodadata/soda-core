#!/usr/bin/env bash

set -e
set -x

cd ..
rm -rf soda_cli
mkdir soda_cli
cd soda_cli

python -m venv .venv

source .venv/bin/activate
pip install --upgrade pip
pip install -i https://pypi.dev.sodadata.io/ soda-core
pip install -i https://pypi.dev.sodadata.io/ soda-postgres
