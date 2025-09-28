#!/usr/bin/env bash

set -e
rm -rf .venv
python3.10 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install "$(grep pip-tools < dev-requirements.in )"
pip-compile dev-requirements.in
pip install -r dev-requirements.txt

# Install all requirements with -e flag
# Pass them all to pip at once so it can resolve dependencies and install in the correct order
pip install $(sed 's/^/-e /' requirements.txt | tr '\n' ' ')