#!/usr/bin/env bash

set -euo pipefail
shopt -s nullglob globstar

if [[ $# -lt 1 ]] || [[ $# -gt 2 ]]; then
  echo "Usage: $0 <version> [--dry-run]" >&2
  exit 1
fi

NEXT="$1"

# Check for dry run flag as second parameter
if [[ $# -eq 2 ]] && [[ "$2" = "--dry-run" ]]; then
  DRY_RUN=true
else
  DRY_RUN=false
fi

# Extract MAJOR.MINOR.PATCH from the start (allowing prerelease/build suffixes)
if [[ "$NEXT" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
  MAJOR="${BASH_REMATCH[1]}"; MINOR="${BASH_REMATCH[2]}"; PATCH="${BASH_REMATCH[3]}"
else
  echo "Skipping due to bad version: '$NEXT'" >&2
  exit 1
fi

echo "Releasing $NEXT"
if [[ "$DRY_RUN" = true ]]; then
  echo "[DRY RUN] Would execute: tbump --non-interactive $NEXT"
  tbump --dry-run $NEXT
else
  tbump --non-interactive "$NEXT"
fi

PRERELEASE_SUFFIX="rc0"
NEXT_PRERELEASE="${MAJOR}.${MINOR}.$((PATCH + 1))${PRERELEASE_SUFFIX}"

# If NEXT is alpha/beta prerelease, skip setting next prerelease
# Matches: 1.2.3a1 / 1.2.3b2  (PEP 440)
if [[ "$NEXT" =~ ^[0-9]+\.[0-9]+\.[0-9]+(a|b|rc)([0-9]+)$ ]]; then
  PRERELEASE_SUFFIX="${BASH_REMATCH[1]}$((BASH_REMATCH[2] + 1))"
  NEXT_PRERELEASE="${MAJOR}.${MINOR}.${PATCH}${PRERELEASE_SUFFIX}"
# Matches: 1.2.3-alpha.1 / 1.2.3-beta.2 (SemVer)
elif [[ "$NEXT" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-alpha|-beta)(\.[0-9]+)?$ ]]; then
  PRERELEASE_SUFFIX="${BASH_REMATCH[1]}$((BASH_REMATCH[2] + 1))"
  NEXT_PRERELEASE="${MAJOR}.${MINOR}.${PATCH}${PRERELEASE_SUFFIX}"
else
  echo "No or unknown prerelease schema; not bumping to next prerelease version."
  exit 1
fi

echo "Setting next prerelease $NEXT_PRERELEASE"
if [[ "$DRY_RUN" = true ]]; then
  echo "[DRY RUN] Would execute: tbump --non-interactive --no-tag $NEXT_PRERELEASE"
  tbump --no-tag --dry-run $NEXT_PRERELEASE
else
  tbump --non-interactive --only-patch "$NEXT_PRERELEASE"
  git commit -a -m "[skip ci] Bump to next prerelease version $NEXT_PRERELEASE"
  git push
fi
