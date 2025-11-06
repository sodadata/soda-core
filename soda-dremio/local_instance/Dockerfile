FROM dremio/dremio-oss:latest

USER root

# Install curl for REST API bootstrap
RUN apt-get update && apt-get install -y curl jq

# Copy Postgres source config
COPY postgres_source.json /opt/dremio/conf/sources/postgres.json

# Copy provisioning script
COPY bootstrap.sh /opt/dremio/bootstrap.sh
RUN chmod +x /opt/dremio/bootstrap.sh

# Change ownership of data directory (helps with the writable issue)
RUN mkdir -p /opt/dremio/data && chown -R 999:999 /opt/dremio/data

USER dremio

# Run Dremio through our bootstrap script
ENTRYPOINT ["/opt/dremio/bootstrap.sh"]
