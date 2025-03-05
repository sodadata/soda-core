# Data source configuration YAML

This page explains how to configure the connection and other data source details.

### Postgres

To configure a connection to a postgres database, use this YAML
```yaml
type: postgres
name: postgres_ds
connection:
  host: localhost
  user: ${POSTGRES_USERNAME}
  password: ${POSTGRES_PASSWORD}
  database: your_postgres_db
```
