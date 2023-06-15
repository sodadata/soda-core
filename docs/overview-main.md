# Soda Core overview

&#10004;  An open-source, CLI tool and Python library for data reliability<br />
&#10004;  Compatible with [Soda Checks Language (SodaCL)](https://docs.soda.io/soda-cl/soda-cl-overview.html) <br />
&#10004;  Enables data quality testing both in and out of your [data pipeline](/docs/orchestrate-scans.md), for data observability and reliability <br />
&#10004;  Enables [programmatic scans](/docs/programmatic.md) on a time-based schedule <br />
<br />

#### Example checks

```yaml
# Checks for basic validations
checks for dim_customer:
  - row_count between 10 and 1000
  - missing_count(birth_date) = 0
  - invalid_percent(phone) < 1 %:
      valid format: phone number
  - invalid_count(number_cars_owned) = 0:
      valid min: 1
      valid max: 6
  - duplicate_count(phone) = 0
checks for dim_product:
  - avg(safety_stock_level) > 50
```

```yaml
# Check for schema changes
checks for dim_product:
  - schema:
      name: Find forbidden, missing, or wrong type
      warn:
        when required column missing: [dealer_price, list_price]
        when forbidden column present: [credit_card]
        when wrong column type:
          standard_cost: money
      fail:
        when forbidden column present: [pii*]
        when wrong column index:
          model_name: 22
```

```yaml
# Check for freshness 
checks for dim_product:
  - freshness(start_date) < 1d
```

```yaml
# Check for referential integrity
checks for dim_department_group:
  - values in (department_group_name) must exist in dim_employee (department_name)
```

## Get started
[Download and install Soda Core](/docs/installation.md) <br />
[Prepare a configuration.yml file](/docs/configuration.md)<br />
[Write checks in a checks.yml file](https://docs.soda.io/soda/quick-start-sodacl.html)<br />
[Run a scan](/docs/scan-core.md)<br />


## Why Soda Core?

Simplify the work of maintaining healthy data and eliminate the bottlenecks in data quality management.
* Download a free, OSS CLI tool and configure settings and data quality checks in two simple YAML files to start scanning your data within minutes.
* Connect Soda Core to over a dozen data sources to scan volumes of data for quality.
* Use the Soda Core Python library to build programmatic scans that you can use in conjunction with orchestration tools like Airflow or Prefect to automate pipeline actions when data quality fails.
* Write data quality checks using SodaCL, a low-code, human-readable, domain-specific language for data quality management.
* Run the same scans for data quality in multiple environments such as development, staging, and production.
