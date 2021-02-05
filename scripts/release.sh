#!/usr/bin/env bash

# Run this from the root project dir with . scripts/run_all_tests.sh

VERSION=2.0.0b6
MESSAGE="Bug fixes and removed metric categories"

git tag -a "$VERSION" -m "$MESSAGE"
git push origin "$VERSION"
