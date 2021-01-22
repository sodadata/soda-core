---
layout: default
title: SQL metrics
parent: Documentation
nav_order: 7
---

# SQL metrics

The most simple SQL metric is selecting 1 numeric value.
For example:

> The name of the field will be used as the name of the metric.  So be sure to 
> provide a meaningful field alias for aggregate functions.  Otherwise you won't 
> be able to properly reference the metrics in the test.

```yaml
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    total_volume_greater_than: total_volume_us > 5000
```

Multiple metrics can be computed with a single query.  Just select multselect fields are supported as well:  

```yaml
sql: |
    SELECT sum(volume) as total_volume_us,
           min(volume) as min_volume_us,
           max(volume) as max_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    total_volume_greater_than: total_volume_us > 5000
    min_volume_greater_than: min_volume_us > 20
    max_volume_greater_than: max_volume_us > 100
    spread_less_than: max_volume_us - min_volume_us < 60 
```

And also group-by sql metrics.  In this case the tests will be checked for 
each group combination.
 
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
    total_volume_greater_than: total_volume > 5000
    min_volume_greater_than: min_volume > 20
    max_volume_greater_than: max_volume > 100
    spread_less_than: max_volume - min_volume < 60 
```