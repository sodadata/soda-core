#!/usr/bin/env bash

set -e

rm -rf .venv

virtualenv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
pip install "$(grep pip-tools < dev-requirements.in )"
pip-compile dev-requirements.in
pip install -r dev-requirements.txt
pip install pre-commit
pip install -e soda-core
pip install -e soda-postgres
