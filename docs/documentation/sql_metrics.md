---
layout: default
title: SQL metrics
parent: Documentation
nav_order: 7
---

# SQL metrics

## Basic metric query

The most simple SQL metric is selecting 1 numeric value.
For example:

> By default the name of the field will be used as the name of the metric.  So  
> using an alias is the simplest way to specify the metric name.  See 
> [Metric names](#metric-names) below if you want to specify the metric name in the 
> YAML 

```yaml
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

## Multiple metrics in one query

Multiple metrics can be computed with a single query.  Just select multselect fields are supported as well:  

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

And also group-by sql metrics.  In this case the tests will be checked for 
each group combination.  In the query, the group by columns have to specified 
as the first fields before the metric fields in the query.
 
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

In case you don't want to use aliases to specify the metric names, 
use `metric_names`.  It's a list of metric names that has to match 
the 

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
