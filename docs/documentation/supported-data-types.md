---
layout: default
title: Data types
parent: Documentation
nav_order: 16
---

# Data types

Soda SQL supports the following data types in the columns it scans. <br />
Currently, Soda SQL does not support complex data types.

### Apache Hive

| Category | Data type | 
| ---- | --------- |
| text | CHAR, VARCHAR |
| number | TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DOUBLE PRECISION, DECIMAL, NUMERIC |

### Amazon Athena

| Category | Data type | 
| ---- | --------- |
| text | CHAR, VARCHAR, STRING |
| number | TINYINT, SMALLINT, INT, INTEGER, BIGINT, DOUBLE, FLOAT, DECIMAL |
| time | DATE, TIMESTAMP |

### Amazon Redshift

| Category | Data type | 
| ---- | --------- |
| text | CHARACTER VARYING, CHARACTER, CHAR, TEXT, NCHAR, NVARCHAR, BPCHAR |
| number | SMALLINT, INT2, INTEGER, INT, INT4, BIGINT, INT8 |
| time | DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ |

### Google Cloud Platform BigQuery

| Category | Data type | 
| ---- | --------- |
| text | STRING |
| number | INT64, DECIMAL, BINUMERIC, BIGDECIMAL, FLOAT64 |
| time | DATE, DATETIME, TIME, TIMESTAMP |

### Postgres

| Category | Data type | 
| ---- | --------- |
| text | CHARACTER VARYING, CHARACTER, CHAR, TEXT |
| number | SMALLINT, INTEGER, BIGINT, DECIMAL, NUMERIC, VARIABLE, REAL, DOUBLE PRECISION, SMALLSERIAL, SERIAL, BIGSERIAL |
| time | TIMESTAMPT, DATE, TIME, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITHOUT TIME ZONE, TIME WITH TIME ZONE, TIME WITHOUT TIME ZONE |

### Snowflake

| Category | Data type | 
| ---- | --------- |
| text | CHAR, VARCHAR, CHARACTER, STRING, TEXT |
| number | NUMBER, INT, INTEGER, BIGINT, SMALLINT, TINYINT, BYTEINT, FLOAT, FLOAT4, FLOAT8, DOUBLE, DOUBLE PRECISION, REAL |
| time | DATE, DATETIME, TIME, TIMESTAMP, TIMESTAMPT_LTZ, TIMESTAMP_NTZ, TIMESTAMP_TZ |

### SQL Server

| Category | Data type | 
| ---- | --------- |
| text | VARCHAR, CHAR, TEXT, NVARCHAR, NCHAR, NTEXT |
| number | BIGINT, NUMERIC, BIT, SMALLINT, DECIMAL, SMALLMONEY, INT, TINYINT, MONEY, FLOAT, REAL |
| time | DATE, DATETIMEOFFSET, DATETIME2, SMALLDATETIME, DATETIME, TIME |