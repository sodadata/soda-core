#!/usr/bin/env bash

# Run this from the root project dir with . scripts/run_all_tests.sh

VERSION=2.0.0b6
MESSAGE="Upgrade dev-requirements.in to their latest patch"

git tag -a $VERSION -m $MESSAGE
git push origin $VERSION
