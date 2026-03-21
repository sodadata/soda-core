#!/usr/bin/env python3
"""Version validation and auto-detection helper for the release workflow.

Usage:
    python3 scripts/validate_version.py --current          # Print current version from __version__.py
    python3 scripts/validate_version.py --resolve          # Auto-detect release version (strip RC suffix)
    python3 scripts/validate_version.py --validate 4.0.8   # Validate version format
    python3 scripts/validate_version.py --next 4.0.8       # Calculate next prerelease version
"""

import argparse
import re
import subprocess
import sys
from pathlib import Path

VERSION_FILE = Path(__file__).resolve().parent.parent / "soda-core" / "src" / "soda_core" / "__version__.py"
VERSION_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')
PRERELEASE_PATTERN = re.compile(r'^(\d+)\.(\d+)\.(\d+)(a|b|rc)(\d+)$')


def read_current_version() -> str:
    content = VERSION_FILE.read_text()
    match = re.search(r'SODA_CORE_VERSION\s*=\s*"(.+?)"', content)
    if not match:
        print(f"::error::Could not parse version from {VERSION_FILE}", file=sys.stderr)
        sys.exit(1)
    return match.group(1)


def resolve_release_version(current: str) -> str:
    """Strip prerelease suffix (rcN, aN, bN) to get the stable release version."""
    stripped = re.sub(r'(a|b|rc)\d+$', '', current)
    if stripped == current:
        print(
            f"::error::Current version '{current}' is already a stable release. "
            "Please provide an explicit version.",
            file=sys.stderr,
        )
        sys.exit(1)
    return stripped


def validate_version(version: str) -> None:
    if not VERSION_PATTERN.match(version):
        print(
            f"::error::Invalid version format '{version}'. "
            "Expected MAJOR.MINOR.PATCH (e.g. 4.0.8)",
            file=sys.stderr,
        )
        sys.exit(1)


def get_bump_level_from_commits() -> str:
    """Inspect conventional commits since the last tag to determine the bump level.

    Returns "minor" if any feat: commits are found, otherwise "patch".
    """
    try:
        last_tag = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
        log_range = f"{last_tag}..HEAD"
    except subprocess.CalledProcessError:
        log_range = "HEAD"

    try:
        log_output = subprocess.run(
            ["git", "log", log_range, "--pretty=format:%s", "--no-merges"],
            capture_output=True, text=True, check=True,
        ).stdout
    except subprocess.CalledProcessError:
        return "patch"

    for line in log_output.splitlines():
        if re.match(r"^feat(?:\([^)]*\))?:", line):
            return "minor"

    return "patch"


def next_prerelease(version: str) -> str:
    m = VERSION_PATTERN.match(version)
    if not m:
        print(f"::error::Cannot compute next prerelease from '{version}'", file=sys.stderr)
        sys.exit(1)
    major, minor, patch = int(m.group(1)), int(m.group(2)), int(m.group(3))

    bump = get_bump_level_from_commits()
    if bump == "minor":
        minor += 1
        patch = 0
    else:
        patch += 1

    return f"{major}.{minor}.{patch}rc0"


def main() -> None:
    parser = argparse.ArgumentParser(description="Version helper for soda-core releases")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--current", action="store_true", help="Print current version")
    group.add_argument("--resolve", action="store_true", help="Auto-detect release version")
    group.add_argument("--validate", metavar="VERSION", help="Validate version format")
    group.add_argument("--next", metavar="VERSION", help="Calculate next prerelease")

    args = parser.parse_args()

    if args.current:
        print(read_current_version())
    elif args.resolve:
        current = read_current_version()
        print(resolve_release_version(current))
    elif args.validate:
        validate_version(args.validate)
        print(args.validate)
    elif args.next:
        print(next_prerelease(args.next))


if __name__ == "__main__":
    main()
