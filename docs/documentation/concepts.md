---
layout: default
title: Concepts
parent: Documentation
nav_order: 1
---

# Concepts

| Concept     | Description |
| ----------- | ----------- |
| Warehouse   | Any SQL-engine or database containing the tables you want to test and monitor |
| Warehouse directory | The top directory in the Soda SQL recommended folder structure containing your `warehouse.yml` file |
| Table | A SQL table |
| Scan YAML | The entrypoint for a `soda scan`, e.g.: `orders.yml` |
| Scan | One Soda SQL computation to extract the configured metrics and tests from a table using SQL queries |
| Column | A SQL column |
| Metric | A metric is property of the data like for example the 'row count' of a table or the 'minimum value' of a column |
| Default metric | A default metric is a Soda SQL provided metric which can be used without any configuration |
| SQL metric | A custom metric which is computed using a user defined SQL query |
| Measurement | One value for a metric obtained during a scan |
| Test | A Python expression that checks if certain expected conditions meet |