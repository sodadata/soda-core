#!/usr/bin/env zsh

# Run this from the root project dir with scripts/release.sh

if [ -z $1 ] || [ -z $2 ]; then
  echo "SYNTAX: scripts/release.sh [RELEASE_TAG_NAME] [RELEASE_TAG_MESSAGE]"
  echo "Eg: scripts/release.sh 2.0.0b9 \"Some release description\""
  exit 1
fi

git tag -a "$1" -m "$2"
git push origin "$1"
