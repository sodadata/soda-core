# Image for the memory_container pytest plugin.
#
# Full workspace install of soda-core / soda-tests / soda-extensions plus
# every adapter the memory_container suite exercises:
#   postgres, sqlserver, snowflake, bigquery, databricks.
# Built editably so that setuptools entry points
# (soda.plugins.check_types.* / soda.plugins.data_source.* etc.) register
# and soda's plugin discovery works.
#
# Source is COPYed to the SAME absolute path the runtime bind mount will use
# (`/Users/niels/Repositories/soda_claude`) so editable `.pth` paths line up
# with the bind mount. Edits to host source thus take effect WITHOUT a rebuild.
# pyproject.toml changes still need a rebuild.
#
# Build is driven from the soda_claude/ parent as build context — see Makefile.
#
# ---------------------------------------------------------------------------
# Analyzing memray.bin files produced by the memory tests
# ---------------------------------------------------------------------------
# memray runs INSIDE this container — never install it on the host venv
# (version skew against the .bin format, venv pollution).
#
# To inspect a memray.bin file from the artifact directory, run a memray
# subcommand inside this image with the artifact dir bind-mounted:
#
#   docker run --rm \
#       -v /path/to/artifact_dir:/work \
#       soda-memtest:latest \
#       python -m memray summary /work/memray.bin
#
# For per-function PEAK-RESIDENT attribution (what cgroup actually sees at
# peak, not lifetime totals), write a small script using memray's Python
# API and mount it alongside the artifact:
#
#   # script using memray.FileReader.get_high_watermark_allocation_records()
#   docker run --rm \
#       -v /path/to/scripts:/scripts \
#       -v /path/to/artifact_dir:/work \
#       soda-memtest:latest \
#       python /scripts/memray_extract_peak.py /work/memray.bin > peak.json
#
# `memray transform csv` is the wrong tool for "where does memory go at peak"
# — it emits LIFETIME totals (every allocation ever made, summed), inflated
# by transients. Use the high-watermark API for peak analysis.
#
# Full template and worked example:
#   soda_claude/memory_management/MEMRAY_DOCKER_ANALYSIS.md
# ---------------------------------------------------------------------------
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive

# System libraries:
#   gcc, python3-dev  → memray native ext (in case the arch lacks a wheel)
#   libpq5            → psycopg[binary] runtime
#   libstdc++6        → memray runtime
#   unixodbc          → pyodbc runtime
#   msodbcsql18       → Microsoft ODBC driver for SQL Server (from MS apt repo)
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates curl gnupg \
        gcc python3-dev \
        libpq5 libstdc++6 \
        unixodbc \
    && curl -s https://packages.microsoft.com/keys/microsoft.asc \
       | gpg --dearmor -o /usr/share/keyrings/microsoft.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Third-party Python deps. Cloud-DB Python drivers are added here so a
# single image supports running memory_container tests against any of the
# supported source adapters. Credentials still come from the host's env
# vars at runtime; this layer only ships the client libraries.
RUN pip install --no-cache-dir \
        pytest==7.4.4 \
        psutil==7.2.2 \
        python-dotenv==1.0.1 \
        freezegun \
        pytz \
        python-dateutil \
        "ruamel.yaml>=0.17.0,<0.18.0" \
        "requests>=2.32.3,<2.34.0" \
        "pydantic>=2.11,<3.0" \
        "opentelemetry-api>=1.16.0,<2.0.0" \
        "opentelemetry-exporter-otlp-proto-http>=1.16.0,<2.0.0" \
        "tabulate[widechars]" \
        sqlglot \
        memray==1.18.0 \
        "psycopg[binary]>=3.2,<4" \
        "pyodbc>=5.0" \
        "pandas>=2.0.0" \
        "pyarrow>=20.0.0" \
        "snowflake-connector-python>=3.0" \
        "google-cloud-bigquery>=2.25.0" \
        "databricks-sql-connector"

# Workspace source. The bind-mount target path is host-specific (developer
# machines vary), so it's an ARG. Default matches the canonical dev layout.
ARG REPO_PARENT=/Users/niels/Repositories/soda_claude

RUN mkdir -p ${REPO_PARENT}
COPY soda-core ${REPO_PARENT}/soda-core
COPY soda-extensions ${REPO_PARENT}/soda-extensions

# Editable installs so entry points register. --no-deps because Python deps
# are already installed above; let the editables just register without
# re-resolving (which would otherwise complain about version pins).
RUN pip install --no-cache-dir --no-deps \
        -e ${REPO_PARENT}/soda-core/soda-core \
        -e ${REPO_PARENT}/soda-core/soda-tests \
        -e ${REPO_PARENT}/soda-core/soda-postgres \
        -e ${REPO_PARENT}/soda-core/soda-sqlserver \
        -e ${REPO_PARENT}/soda-core/soda-snowflake \
        -e ${REPO_PARENT}/soda-core/soda-bigquery \
        -e ${REPO_PARENT}/soda-core/soda-databricks \
        -e ${REPO_PARENT}/soda-extensions/soda

# WORKDIR is set at runtime via `docker run -w` to mirror the host rootdir.
