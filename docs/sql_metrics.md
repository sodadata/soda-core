# SQL metrics

The most simple SQL metric is selecting 1 numeric value.
The name of the field will be used as the metric name in the tests.
For example:

```yaml
sql: |
    SELECT sum(volume) as total_volume_us
    FROM CUSTOMER_TRANSACTIONS
    WHERE country = 'US'
tests:
    - total_volume_us > 5000
```

Multiple select fields are supported as well:  

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
    - total_volume > 5000
    - min_volume > 20
    - max_volume > 100
    - max_volume - min_volume < 60 
```
