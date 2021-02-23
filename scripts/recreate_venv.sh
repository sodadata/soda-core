#!/usr/bin/env bash

set -e

# Run this from the root project dir with scripts/recreate_venv.sh

VENV_DIR=.venv

rm -rf $VENV_DIR
rm -rf soda_sql.egg-info

python3 -m venv $VENV_DIR
source $VENV_DIR/bin/activate
pip install --upgrade pip
pip install "$(cat dev-requirements.in | grep pip-tools)"
pip-compile requirements.in
pip-compile dev-requirements.in
pip install -r requirements.txt
pip install -r dev-requirements.txt
pip install -e .
