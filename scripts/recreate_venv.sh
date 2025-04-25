#!/usr/bin/env bash

set -e

rm -rf .venv

# pyenv virtualenv .venv
python -m venv .venv

# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip
pip install "$(grep pip-tools < dev-requirements.in )"
pip-compile dev-requirements.in
pip install -r dev-requirements.txt

cat requirements.txt | while read requirement || [[ -n $requirement ]];
do
   pip install -e $requirement
done
