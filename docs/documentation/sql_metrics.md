---
layout: default
title: SQL metrics
parent: Documentation
nav_order: 7
---

# SQL metrics

Soda SQL comes shipped with the capability to extend our default set of common metrics. This
allows you to compose tests based on custom-made optimized for your dataset.

Creating new custom SQL metrics is as simple as creating an additional `.yml` file in the
directory containing the `scan.yml`. Soda SQL will then automatically recognize these files.

An example of such a directory structure may look like:
```
- your_project/
  - warehouse.yml
  - orders/
    - scan.yml
    - total_volume_us.sql
```

## Basic metric query

The most simple SQL metric is selecting 1 numeric value.
For example:

> By default the name of the field will be used as the name of the metric.  So
> using an alias is the simplest way to specify the metric name.  See
> [Metric names](#metric-names) below if you don't want to specify aliases in your query.


```yaml
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

## Multiple metrics in one query

You can also compute multiple metric values in a single query. These values can then be combined in your tests:

```yaml
sql: |
    SELECT sum(volume) as total_volume_us,
           min(volume) as min_volume_us,
           max(volume) as max_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
    - min_volume_us > 20
    - max_volume_us > 100
    - max_volume_us - min_volume_us < 60
```

## Group by queries

It's possible to define group-by sql metrics which allows each test to be checked against
each group combination.  In order for Soda SQL to understand that you're using a
`GROUP BY` it's important to specify the fields you're grouping on. This is done
by setting the `group_fields` property:

```yaml
sql: |
    SELECT country,
           sum(volume) as total_volume,
           min(volume) as min_volume,
           max(volume) as max_volume
    FROM CUSTOMER_TRANSACTIONS
    GROUP BY country
group_fields:
    - country
tests:
    - total_volume > 5000
    - min_volume > 20
    - max_volume > 100
    - max_volume - min_volume < 60
```

## Metric names

Defining aliases in your `SELECT` statement is optional. In case you don't want to
do so you'll have to provide the `metric_names` property. This property contains
a list of values which should match the order of values in your `SELECT` statement:

```yaml
sql: |
    SELECT sum(volume),
           min(volume),
           max(volume)
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
metric_names:
    - total_volume_us
    - min_volume_us
    - max_volume_us
tests:
    - total_volume_us > 5000
    - min_volume_us > 20
    - max_volume_us > 100
    - max_volume_us - min_volume_us < 60
```
