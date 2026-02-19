#!/usr/bin/env python3
"""Generate structured release notes from conventional commits since the last tag.

Groups commits by type (feat, fix, chore, etc.) and lists contributors.

Usage:
    python3 scripts/generate_release_notes.py [--version VERSION]

Output is Markdown written to stdout.
"""

import argparse
import re
import subprocess
import sys
from collections import defaultdict

# Conventional commit prefix → display label (order matters for output)
CATEGORIES = [
    ("feat", "Features"),
    ("fix", "Bug Fixes"),
    ("perf", "Performance"),
    ("refactor", "Refactoring"),
    ("docs", "Documentation"),
    ("test", "Tests"),
    ("ci", "CI/CD"),
    ("build", "Build"),
    ("chore", "Chores"),
]

CATEGORY_MAP = dict(CATEGORIES)

# Pattern: "type: description" or "type(scope): description"
CONVENTIONAL_RE = re.compile(r"^(\w+)(?:\([^)]+\))?:\s*(\S.*)$")


def git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def get_last_tag() -> str | None:
    try:
        return git("describe", "--tags", "--abbrev=0", "HEAD^")
    except subprocess.CalledProcessError:
        return None


def get_commits_since(ref: str | None) -> list[dict]:
    if ref:
        log_range = f"{ref}..HEAD"
    else:
        log_range = "HEAD"

    # format: hash<SEP>author<SEP>subject
    SEP = "<||>"
    raw = git("log", log_range, f"--pretty=format:%h{SEP}%an{SEP}%s", "--no-merges")
    if not raw:
        return []

    commits = []
    for line in raw.splitlines():
        parts = line.split(SEP, 2)
        if len(parts) == 3:
            commits.append({"hash": parts[0], "author": parts[1], "subject": parts[2]})
    return commits


def categorize(commits: list[dict]) -> tuple[dict[str, list[dict]], list[dict]]:
    grouped: dict[str, list[dict]] = defaultdict(list)
    uncategorized: list[dict] = []

    for commit in commits:
        # Skip version bump commits
        if commit["subject"].startswith(("[skip ci]", "Bump to ")):
            continue

        m = CONVENTIONAL_RE.match(commit["subject"])
        if m:
            prefix = m.group(1).lower()
            if prefix in CATEGORY_MAP:
                grouped[prefix].append(commit)
                continue
        uncategorized.append(commit)

    return grouped, uncategorized


def render(version: str | None, grouped: dict, uncategorized: list, contributors: set) -> str:
    lines: list[str] = []

    if version:
        lines.append(f"## What's Changed in {version}")
    else:
        lines.append("## What's Changed")
    lines.append("")

    for prefix, label in CATEGORIES:
        if prefix not in grouped:
            continue
        lines.append(f"### {label}")
        for c in grouped[prefix]:
            lines.append(f"- {c['subject']} ({c['hash']})")
        lines.append("")

    if uncategorized:
        lines.append("### Other Changes")
        for c in uncategorized:
            lines.append(f"- {c['subject']} ({c['hash']})")
        lines.append("")

    if contributors:
        lines.append("### Contributors")
        for name in sorted(contributors):
            lines.append(f"- {name}")
        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate release notes")
    parser.add_argument("--version", help="Release version for the heading")
    args = parser.parse_args()

    last_tag = get_last_tag()
    commits = get_commits_since(last_tag)

    if not commits:
        if args.version:
            print(f"## {args.version}\n\nNo changes since last release.")
        else:
            print("No changes since last release.")
        return

    grouped, uncategorized = categorize(commits)
    contributors = {c["author"] for c in commits}

    print(render(args.version, grouped, uncategorized, contributors))


if __name__ == "__main__":
    main()
