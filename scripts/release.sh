#!/usr/bin/env bash

# Run this from the root project dir with scripts/run_all_tests.sh

VERSION=2.0.0b5
MESSAGE="Fixed Snowflake CLI issue #65"

git tag -a $VERSION -m $MESSAGE
git push origin $VERSION
