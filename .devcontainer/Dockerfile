FROM mcr.microsoft.com/devcontainers/python:1-3.9-bookworm

ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=Etc/UTC
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    procps \
    build-essential curl \
    libpq-dev \
    libssl-dev libffi-dev \
    unixodbc-dev git lsb-release \
    alien odbcinst

# Install Microsoft ODBC driver 18 for SQL Server https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    echo "deb [arch=$(dpkg --print-architecture)] https://packages.microsoft.com/debian/12/prod $(lsb_release -cs) main" > /etc/apt/sources.list.d/microsoft-prod.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y libsasl2-dev odbcinst mssql-tools18 msodbcsql18 unixodbc-dev

# Dremio support
RUN curl -L https://download.dremio.com/arrow-flight-sql-odbc-driver/arrow-flight-sql-odbc-driver-LATEST.x86_64.rpm -o arrow-driver.rpm && \
    alien -iv --scripts arrow-driver.rpm

# Run sleep infinity so that the dev container stays alive
CMD [ "sleep", "infinity" ]
