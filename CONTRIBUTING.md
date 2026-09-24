# Contributing to Soda Core

Thanks for taking the time to contribute. Soda Core is an open-source data quality and data contract verification engine, and we welcome bug reports, fixes, documentation improvements, and new extensions.

This guide covers everything you need to know to get a local development environment running, the conventions we follow, and how to submit a change. It is written for humans, but it is also intended to be readable by coding agents (Claude Code, Cursor, …) that may drive parts of a contribution end-to-end.

---

## Where to ask questions

- **Bug reports** and **feature requests**: open a [GitHub issue](https://github.com/sodadata/soda-core/issues).
- **General questions** about using Soda Core: join the [Soda Community Slack](https://soda-community.slack.com/).
- **Security issues**: do **not** open a public issue. Email security@soda.io.

Before filing a bug, check existing issues and the [Soda documentation](https://docs.soda.io/soda-v4/) — your question may already be answered.

---

## What you can contribute

There are several ways to contribute to Soda Core. In rough order from smallest to largest:

- **Bug fixes** and **documentation improvements** — always welcome. Open a PR directly.
- **New built-in check types** that are broadly useful (e.g. a new statistical check, a new validity rule). Discuss in an issue first so we can confirm the scope is open-source.
- **New data sources** — adding a new database to the supported list. This is the most common contribution from outside the core team. See [`CONTRIBUTING-DATA-SOURCE.md`](CONTRIBUTING-DATA-SOURCE.md) for the full guide.
- **External extension packages** — Soda Core has a public plugin system based on Python entry points. You can ship your own data source or check-type plugin as a separate PyPI package without merging anything upstream. The same base classes documented in [`CONTRIBUTING-DATA-SOURCE.md`](CONTRIBUTING-DATA-SOURCE.md) apply.

For anything beyond a small bug fix, please open an issue first to align on the approach before writing code.

---

## How extensions work

Soda Core has two extension surfaces:

1. **Data sources** — teach Soda how to connect to a database, render its SQL dialect, and query its metadata. Every contract verification runs through a `DataSourceImpl` + `SqlDialect` + `DataSourceConnection` triple.
2. **Check types** — add a new kind of check that contract authors can write in YAML.

Both surfaces use the same mechanism: a Python entry point under the `soda.plugins.*` group, discovered at runtime by `soda_core.plugins.load_plugins()`. An extension package can ship one, the other, or both.

For a step-by-step guide to building either, see [`CONTRIBUTING-DATA-SOURCE.md`](CONTRIBUTING-DATA-SOURCE.md) (the human guide) and [`CONTRIBUTING-DATA-SOURCE.AGENT.md`](CONTRIBUTING-DATA-SOURCE.AGENT.md) (the dense reference). Both files live in this repository.

---

## Development setup

### Requirements

- Python 3.10 or newer.
- [UV](https://docs.astral.sh/uv/) (recommended) or pip 21.0+.

### Install

```bash
# clone the repo, then from the repo root:
uv sync --all-packages --group dev
```

This installs every package in the workspace (`soda-core`, `soda-tests`, all data source packages) in editable mode with their development dependencies.

Pip equivalent (if you don't have UV). Unlike `uv sync --all-packages`, this only installs the packages you list — add `-e soda-{name}` for every driver you need:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e soda-core -e soda-tests \
            -e soda-postgres   # add -e soda-snowflake / soda-bigquery / ... as needed
pip install pytest pre-commit pydantic python-dotenv freezegun
```

### Tests

```bash
# unit tests — fast, no database needed
uv run pytest soda-tests/tests/unit

# data-source-specific tests (Postgres ships with a docker-compose.yml)
uv run pytest soda-postgres/tests

# cross-data-source suite against a specific engine
TEST_DATASOURCE=postgres uv run pytest soda-tests/tests
```

Credentials for cloud-only data sources go in a `.env` file at the repo root — see `.env_example`. The file is gitignored.

### What not to commit

- **No credentials anywhere in tracked files.** `.env` is gitignored — keep it that way. Service-account JSON, API tokens, and passwords belong in `.env` (locally) or in CI secrets (in CI), never in code, YAML, or markdown.
- **No real credentials in `docker-compose.yml`.** Use placeholder values that are obviously placeholders (`POSTGRES_PASSWORD: postgres`). Reviewers will reject anything that looks like a real secret.
- **No personal absolute paths** (e.g. `/Users/you/...`) in committed code or docs. Use repo-relative paths.

### Matching the soda-core version

A new data source package must pin `soda-core` to the current workspace version. Find it with:

```bash
grep -m1 '^version' soda-core/pyproject.toml
```

Use that exact string in your `pyproject.toml` dependencies and in your own `version` field.

### Pre-commit checks

We use `pre-commit` to enforce formatting and basic hygiene. Install the hook once:

```bash
uv run pre-commit install
```

…and run it before pushing:

```bash
uv run pre-commit run --all-files
```

---

## Code style

Enforced automatically by pre-commit:

- **black**, line length 120
- **isort**, black profile
- **autoflake** for unused imports
- standard hygiene checks (trailing whitespace, YAML/JSON/TOML syntax, no debug statements)

Beyond formatting:

- Write SQL via the `sql_ast` builders in `soda_core.common.sql_ast`, not by string concatenation. The builders go through dialect rules; raw strings bypass them.
- Use frozen pydantic models for YAML-facing data shapes — see `soda_core/model/`. Top-level model classes (e.g. `DataSourceBase`) use `extra="forbid"`; connection-properties subclasses use `extra="allow"` so unknown driver kwargs flow through unchanged.
- Type-hint public functions. The codebase is gradually moving toward stricter typing.
- Keep comments for *why*, not *what*. Code reviewers will push back on commentary that restates the code.

---

## Submitting a pull request

1. **Fork** the repo and create a topic branch.
2. **Open a draft PR early** if the change is more than a few lines. It costs nothing and lets maintainers flag dead ends before you sink time into them.
3. **Write a clear PR description**:
    - what the change does and why,
    - which tests cover it,
    - anything you deliberately did *not* do (and why),
    - any tests you skipped, with reasons.
4. **Run the checks locally**:
    ```bash
    uv run pre-commit run --all-files
    uv run pytest soda-tests/tests/unit
    # plus any data-source or integration tests relevant to your change
    ```
5. **Keep the PR focused**. One logical change per PR. Unrelated cleanups go in a separate PR.

We squash-merge most PRs. Your commit history within the PR doesn't need to be pristine, but the final squashed commit message should be.

---

## Versioning and releases

Soda Core uses semantic versioning, managed via `tbump`. Releases are cut by Soda maintainers — contributors do not need to update version numbers in their PRs.

---

## License

Soda Core is licensed under the Apache License 2.0. By submitting a contribution, you agree that your work will be released under the same license. See [`LICENSE`](LICENSE) for the full text.

---

## A note for coding agents

If you are a coding agent (Claude Code, Cursor, etc.) driving a contribution:

- **Read the base classes before writing code.** Headers and signatures are documented in [`CONTRIBUTING-DATA-SOURCE.AGENT.md`](CONTRIBUTING-DATA-SOURCE.AGENT.md), but the actual code in `soda-core/src/soda_core/common/` is the source of truth. When the docs and the code disagree, the code wins.
- **Start from a working reference.** Copy `soda-duckdb/` (minimal) or `soda-postgres/` (full-featured) and adapt, rather than generating files from scratch.
- **Verify before claiming done.** Run `uv run pre-commit run --all-files` and the relevant pytest paths. Do not declare a task complete unless the commands actually pass.
- **Do not edit `soda-core/` itself** unless you found a real upstream bug. Extensions live in their own packages.
