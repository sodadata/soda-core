# Image for the memory_container pytest plugin.
#
# Holds enough to invoke `pytest <nodeid>` on a body that doesn't touch the
# database (Phase 1). Plus memray for opt-in flamegraph profiling (Phase 2B).
# System libs for soda's data-source adapters (libpq, ODBC, etc.) and the
# workspace `pip install -e` come in a later phase.
FROM python:3.12-slim

# Memray needs gcc + python headers to build extensions on some architectures
# where prebuilt wheels are absent. Slim base lacks gcc; install it just-in-time.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        python3-dev \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

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
        memray==1.18.0

# CWD is set at runtime via `docker run -w` to mirror the host rootdir, so
# pytest finds the project's conftest.py / pytest.ini at the same path
# inside the container as it would on the host.
