# How Soda Core works 

**Soda Core** is a free, open-source command-line tool. It utilizes user-defined input to prepare SQL queries that run checks on datasets in a data source to find invalid, missing, or unexpected data. 

When checks fail, they surface the data that you defined as "bad" in the check. Armed with this information, you and your data engineering team can diagnose where the "bad" data entered your data pipeline and take steps to prioritize and resolve issues.

Use Soda Core on its own to manually or programmatically scan the data that your organization uses to make decisions. Optionally, you can integrate Soda Core with your data orchestration tool to schedule scans and automate actions based on scan results. 


[Soda Core basics](#soda-core-basics)
[Soda Core operation](#soda-core-operation)
[Soda Core automation and integrations](#soda-core-automation-and-integrations)


## Soda Core basics

This open-source, command-line tool exists to enable Data Engineers to access and check data inside data sources. It enables you to perform three basic tasks:

- connect to your data source
- define checks to surface bad-quality data
- scan your dataset to run checks against your data


To connect to a data source such as Snowflake, Amazon Athena, or GCP Big Query, you use a `configuration.yml` file which stores access details for your data source. (Except for connections to Spark DataFrames which do not use a configuration YAML file.) See [Configure Soda Core](/docs/configuration.md) for details.

#### Configuration YAML example

```yaml
data_source adventureworks:
  type: postgres
  connection:
    host: localhost
    port: '5432'
    username: postgres
    password: secret
  database: postgres
  schema: public
```

<br />

To define the data quality checks that Soda Core runs against a dataset, you use a `checks.yml` file. A Soda Check is a test that Soda Core performs when it scans a dataset in your data source. The checks YAML file stores the Soda Checks you write using [SodaCL](https://docs.soda.io/soda-cl/soda-cl-overview.html). 

For example, you can define checks that look for things like missing or forbidden columns in a dataset, or rows that contain data in an invalid format. See [Metrics and checks](https://docs.soda.io/soda-cl/metrics-and-checks.html) for more details.

#### Checks YAML example

```yaml
# Check for absent or forbidden columns in dataset
checks for dataset_name:
  - schema:
      warn:
        when required column missing: [column_name]
      fail:
        when forbidden column present: [column_name, column_name2]

# Check an email column to confirm that all values are in email format
checks for dataset_name:
  - invalid_count(email_column_name) = 0:
      valid format: email
```

In your own local environment, you create and store your checks YAML file anywhere you wish, then identify its name and filepath in the scan command. In fact, you can name the file whatever you like, as long as it is a `.yml` file and it contains checks using the SodaCL syntax.

You write Soda Checks using SodaCL’s built-in metrics, though you can go beyond the built-in metrics and write your own SQL queries, if you wish. The example above illustrates two simple checks on two datasets, but SodaCL offers a wealth of built-in metrics that enable you to define checks for more complex situations.

<br />

To scan your data, you use the `soda scan` CLI command. Soda Core uses the input in the checks YAML file to prepare SQL queries that it runs against the data in one or more datasets in a data source. It returns the output of the scan with each check's results in the CLI. See [Anatomy of a scan command](/docs/scan-core.md#anatomy-of-a-scan-command) for more details.

```shell
soda scan -d adventureworks -c configuration.yml checks.yml
```

## Soda Core operation

The following image illustrates what Soda Core does when you initiate a scan.

![soda-core-operation](/docs/assets/images/soda-core-operation.png)

**1** - You trigger a scan using the `soda scan` CLI command from your Soda project directory which contains the `configuration.yml` and `checks.yml` files. The scan specifies which data source to scan, where to get data source access info,  and which checks to run on which datasets.

**2** - Soda Core uses the checks you defined in the checks YAML to prepare SQL queries that it runs on the datasets in your data source.

**3** - When Soda Core runs a scan, it performs the following actions:
- fetches column metadata (column name, type, and nullable)
- executes a single aggregation query that computes aggregate metrics for multiple columns, such as `missing`, `min`, or `max`
- for each column each dataset, executes several more queries

**4** - As a result of a scan, each check results in one of three default states:
- **pass**: the values in the dataset match or fall within the thresholds you specified
- **fail**: the values in the dataset do not match or fall within the thresholds you specified
- **error**: the syntax of the check is invalid

A fourth state, **warn**, is something you can explicitly configure for individual checks. See [Add alert configurations](https://docs.soda.io/soda-cl/optional-config.html#add-alert-configurations).

The scan results appear in your command-line interface (CLI).
```shell
Soda Core 3.0.xx
Scan summary:
1/1 check PASSED: 
    dim_customer in adventureworks
      row_count > 0 [PASSED]
All is good. No failures. No warnings. No errors.
Sending results to Soda Cloud
```


## Soda Core automation and integrations

To automate scans on your data, you can use the **Soda Core Python library** to programmatically execute scans. Based on a set of conditions or a specific schedule of events, you can instruct Soda Core to automatically run scans. 

For example, you may wish to scan your data at several points along your data pipeline, perhaps when new data enters a warehouse, after it is transformed, and before it is exported to another warehouse. Refer to the [Define programmatic scans](/docs/programmatic.md) instructions for details.

Alternatively, you can integrate Soda Core with a **data orchestration tool** such as, Airflow, Dagster, or dbt Core, to schedule automated scans. You can also configure actions that the orchestration tool can take based on scan output. 

For example, if the output of a scan reveals a large number of failed tests, the orchestration tool can automatically quarantine the bad-quality data or block it from contaminating your data pipeline. Refer to [Orchestrate scans](/docs/orchestrate-scans.md) for details.