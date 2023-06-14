# Migrate from Soda SQL to Soda Core 

Soda SQL, the original command-line tool that Soda created to test for data quality, has been deprecated and replaced by [Soda Core](/docs/overview-main.md). As the primary tool for data reliability, you can install Soda Core, connect to your data sources, and convert your Soda SQL tests into SodaCL checks. 

Use the following guide to understand the migration path from Soda SQL to Soda Core. <br />Contact <a href="mailto:support@soda.io">support@soda.io</a> for help migrating from Soda SQL to Soda Core. 


## Migrate to Soda Core

1. [Install](/docs/installation.md) Soda Core.
2. [Configure](/docs/configuration.md) Soda Core to connect to your data sources.
3. Convert your Soda SQL tests in your scan YAML file to SodaCL checks in a checks YAML file. Refer to [SodaCL documentation](https://docs.soda.io/soda-cl/metrics-and-checks.html) and some [example conversions](#example-conversions-from-tests-to-checks), below.<br />
4. Use Soda Core to [run fresh scans](/docs/scan-core.md) of data in your data sources.
5. (Optional) Revise your [programmatic scans](/docs/programmatic.md) to use Soda Core.

### Example conversions from tests to checks

```yaml
# SODA SQL
table_name: orders.yaml
metrics
  - row_count
  - missing_count
  - missing_percentage
  - ...
columns:
  ID:
    metrics:
      - distinct
      - duplicate_count
      - valid_count
      - avg
    historic metrics:
      - name: avg_dup_7
        type: avg
        metric: duplicate_count
        count: 7
    tests:
      - duplicate_count < avg_dup_7

# SODA CORE
checks for orders:
  - change avg last 7 for duplicate_count < 50
```

<br />

```yaml
# SODA SQL
table_name: fulfillment
metrics:
  - row_count
  - missing_count
  - missing_percentage
  - values_count
  - ...
columns:
  country:
    valid_values:
      - US
      - UK
      - CN
    tests:
      - invalid_percentage == 0

# SODA CORE
checks for fulfillment:
  - invalid_percent(country) = 0%:
      valid values: [US, UK, CN]
```
