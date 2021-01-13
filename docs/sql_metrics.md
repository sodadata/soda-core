# SQL metrics

Soda SQL provides you with a list of predefined metrics. In addition to these
metrics you can also write your own, custom, SQL metrics.

The most simple implementation of a custom SQL metric implements a SQL-query which
select 1 numeric value. This number will be set as the metric's value.

The name of the field will be used as the metric name. This name can then later be used
in your [tests](tests.md).

For example:

```yaml
metrics:
    - total_volume_us
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

Multiple select fields are supported as well:

```yaml
metrics:
    - total_volume_us,
    - min_volume_us,
    - max_volume_us
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

And also group-by sql metrics.  In this case the tests will be checked for
each group combination.

```yaml
metrics:
    - total_volume,
    - min_volume,
    - max_volume
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
