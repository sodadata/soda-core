#!/usr/bin/env bash

set -e
rm -rf .venv
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "$(grep pip-tools < dev-requirements.in )"
pip-compile dev-requirements.in
pip install -r dev-requirements.txt

pip install $(cat requirements.txt | sed 's/^/-e /' | tr '\n' ' ')
