---
layout: default
title: Concepts
parent: Documentation
nav_order: 1
---

# Concepts

| Concept     | Description |
| ----------- | ----------- |
| Warehouse   | Any SQL-engine or DB accessible over SQL queries that contains the tables you want to test and monitor |
| Warehouse directory | The top directory in the Soda SQL recommended folder structure containing the warehouse.yml and the table directories |
| Table | A SQL table |
| Table directory | A sub directory of a warehouse directory containing the scan.yml and sql metrics for that table |
| Column | A SQL column |
| Metric | A metric is property of a the data like Eg the row count of a table or the minimum value of a column |
| Default metric | A default metric is a metric that Soda SQL already knows how to compute through SQL |
| SQL metric | A metric that is computed with a user defined SQL query |
| Scan | One Soda SQL computation to extract the configured metrics and tests from a table using SQL queries |
| Measurement | One value for a metric obtained during a scan |
| Test | A Python expression that checks if certain expected conditions |