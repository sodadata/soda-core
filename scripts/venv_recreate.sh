#!/usr/bin/env bash

set -e

rm -rf .venv
rm -rf soda_sql.egg-info

virtualenv .venv
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
pip install "$(grep pip-tools < dev-requirements.in )"
pip-compile dev-requirements.in
pip install -r dev-requirements.txt
pip install pre-commit
pip install -e contracts
pip install -e contracts_tests
pip install -e contracts_postgres
