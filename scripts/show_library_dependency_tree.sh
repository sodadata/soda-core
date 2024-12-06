#!/usr/bin/env bash

set -e
source .venv/bin/activate
pip install pipdeptree
pipdeptree -fl
