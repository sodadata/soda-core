---
layout: default
title: Run a Soda Core scan
description:  Soda Core uses the input in the checks and configuration YAML file to prepare a scan that it runs against the data in a dataset.
parent: Soda Core
redirect_from:
- /soda-core/first-scan.html
- /soda-core/scan-reference.html
---

# Run a Soda Core scan 
*Last modified on {% last_modified_at %}*

A **scan** is a command that executes checks to extract information about data in a dataset. 
{% include code-header.html %}
```shell
soda scan -d postgres_retail_db  -c configuration.yml checks.yml
```

A **check** is a test that Soda Core performs when it scans a dataset in your data source. Soda Core uses the checks you define in the **checks YAML** file to prepare SQL queries that it runs against the data in a table. Soda Core can execute multiple checks against one or more datasets in a single scan. 

[Anatomy of a scan command](#anatomy-of-a-scan-command)<br />
[Variables](#variables)<br />
[Scan output](#scan-output)<br />
[Examine a scan's SQL queries](#examine-a-scans-sql-queries)<br />
[Programmatically use scan output](#programmatically-use-scan-output)<br />
[Configure the same scan to run in multiple environments](#configure-the-same-scan-to-run-in-multiple-environments)<br />
[Add scan options](#add-scan-options)<br />
[Troubleshoot](#troubleshoot)<br />
[Go further](#go-further) <br />
<br />

## Anatomy of a scan command

Each scan requires the following as input:

* the name of the data source that contains the dataset you wish to scan, identified using the `-d` option
* a `configuration.yml` file, which contains details about how Soda Core can connect to your data source, identified using the `-c` option
* a `checks.yml` file which contains the checks you write using SodaCL

Scan command:
{% include code-header.html %}
```shell
soda scan -d postgres_retail -c configuration.yml checks.yml
```
<br />

Note that you can use the `-c` option to include **multiple configuration YAML files** in one scan execution. Include the filepath of each YAML file if you stored them in a directory other than the one in which you installed Soda Core.
{% include code-header.html %}
```shell
soda scan -d postgres_retail -c other-directory/configuration.yml other-directory/checks.yml
```

<br />

You can also include **multiple checks YAML files** in one scan execution. Use multiple checks YAML files to execute different sets of checks during a scan. 
{% include code-header.html %}
```shell
soda scan -d postgres_retail -c configuration.yml checks_stats1.yml checks_stats2.yml
```

<br />
Use the soda `soda scan --help` command to review options you can include to customize the scan. See also: [Add scan options](#add-scan-options).

## Variables

There are several ways you can use variables in checks, filters, and in your data source configuration to pass values at scan time; a few examples follow. 

Refer to the comprehensive [Filters and variables]({% link soda-cl/filters.md %}) documentation for details.
{% include code-header.html %}
```yaml
# In-check filter
checks for dim_employee:
  - max(vacation_hours) < 80:
      name: Too many vacation hours for US Sales
      filter: sales_territory_key = 11

# Dataset filter with variables
filter CUSTOMERS [daily]:
  where: TIMESTAMP '${ts_start}' <= "ts" AND "ts" < TIMESTAMP '${ts_end}'

checks for CUSTOMERS [daily]:
  - row_count = 6
  - missing(cat) = 2

# In-check variable 
checks for ${DATASET}:
  - invalid_count(last_name) = 0:
      valid length: 10 
```


## Scan output

{% include scan-output.md %}

Example output with a check that triggered a warning:
```shell
Soda Core 0.0.x
Scan summary:
1/1 check WARNED: 
    CUSTOMERS in postgres_retail
      schema [WARNED]
        missing_column_names = [sombrero]
        schema_measured = [geography_key, customer_alternate_key, title, first_name, last_name ...]
Only 1 warning. 0 failure. 0 errors. 0 pass.
```

Example output with a check that failed:
```shell
Soda Core 0.0.x
Scan summary:
1/1 check FAILED: 
    CUSTOMERS in postgres_retail
      freshness(full_date_alternate_key) < 3d [FAILED]
        max_column_timestamp: 2020-06-24 00:04:10+00:00
        max_column_timestamp_utc: 2020-06-24 00:04:10+00:00
        now_variable_name: NOW
        now_timestamp: 2022-03-10T16:30:12.608845
        now_timestamp_utc: 2022-03-10 16:30:12.608845+00:00
        freshness: 624 days, 16:26:02.608845
Oops! 1 failures. 0 warnings. 0 errors. 0 pass.

```

### Examine a scan's SQL queries

To examine the SQL queries that Soda Core prepares and executes as part of a scan, you can add the `-V` option to your `soda scan` command. This option prints the queries as part of the scan results.
{% include code-header.html %}
```shell
soda scan -d postgres_retail -c configuration.yml -V checks.yml
``` 

## Programmatically use scan output

Optionally, you can insert the output of Soda Core scans into your data orchestration tool such as Dagster, or Apache Airflow. 

You can save Soda Core scan results anywhere in your system; the `scan_result` object contains all the scan result information. To import the Soda Core library in Python so you can utilize the `Scan()` object, [install a Soda Core package]({% link soda-core/installation.md %}), then use `from soda.scan import Scan`. Refer to [Define programmatic scans]({% link soda-core/programmatic.md %}) for details.

Further, in your orchestration tool, you can use Soda Core scan results to block the data pipeline if it encounters bad data, or to run in parallel to surface issues with your data. Learn how to [Configure orchestrated scans]({% link soda-core/orchestrate-scans.md %}).

## Configure the same scan to run in multiple environments

{% include scan-multiple-envs.md %}

## Add scan options

When you run a scan in Soda Core, you can specify some options that modify the scan actions or output. Add one or more of the following options to a `soda scan` command.

| Option | Required | Description and examples |
| ------ | :------: | ---------------------- |
| `-c TEXT` or<br /> `--configuration TEXT` | ✓ | Use this option to specify the file path and file name for the configuration YAML file.|
| `-d TEXT` or<br /> `--data-source TEXT` |  ✓ | Use this option to specify the data source that contains the datasets you wish to scan.|
| `-s TEXT` or<br /> `--scan-definition TEXT` |  | Use this option to provide a [scan definition name]({% link soda/glossary.md %}#scan-definition-name) so that Soda Cloud keeps check results from different environments (dev, prod, staging) separate. See [Configure a single scan to run in multiple environments]({% link soda-core/configuration.md %}#configure-the-same-scan-to-run-in-multiple-environments).|
| `-srf` or <br /> `--scan-results-file TEXT` |  | Specify the file name and file path to which Soda Core sends a JSON file of the scan results. You can use this in addition to, or instead of, sending results to Soda Cloud. <br /> `soda scan -d adventureworks -c configuration.yml -srf test.json checks.yml`|
| `-t TEXT` or<br /> `--data-timestamp TEXT` |  | Placeholder only. |
| `-v TEXT` or<br /> `--variable TEXT` |  | Replace `TEXT` with variables you wish to apply to the scan, such as a [filter for a date]({% link soda-cl/filters.md %}). Put single or double quotes around any value with spaces. <br />  `soda scan -d my_datasource -v start=2020-04-12 -c configuration.yml checks.yml` |
| `V` or <br /> `--verbose` |  | Return scan output in verbose mode to review query details. |

## Troubleshoot

**Problem:** When you run a scan, you get an error that reads, `Exception while exporting Span batch.`

**Solution:** Without an internet connection, Soda Core is unable to communicate with `soda.connect.io` to transmit anonymous usage statistics about the software. <br /> If you are using Soda Core offline, you can resolve the issue by setting `send_anonymous_usage_stats: false` in your `configuration.yml` file. Refer to [Soda Core usage statistics]({% link soda-core/usage-stats.md %}) for further details.

<br />

**Problem:** When you run a scan using Soda Core 3.0.9, you get an error message that reads, `from google.protobuf.pyext import _message ImportError: dlopen(.../site-packages/google/protobuf/pyext/_message.cpython-310-darwin.so, 0x0002): symbol not found in flat namespace`

**Solution:** This is the result of a transitive dependency from open telemetry that gathers OSS usage statistics. To resolve:
1. From the command-line, in the directory in which you installed your soda-core package, run `pip uninistall protobuf`.
2. Reinstall protobuf with the command `pip install protobuf==3.19.4`.


## Go further

* Consider completing the [Quick start for SodaCL]({% link soda/quick-start-sodacl.md %}) to learn how to write more checks for data quality.
* Need help? Join the <a href="https://community.soda.io/slack" target="_blank"> Soda community on Slack</a>.
<br />

---

Was this documentation helpful?

<!-- LikeBtn.com BEGIN -->
<span class="likebtn-wrapper" data-theme="tick" data-i18n_like="Yes" data-ef_voting="grow" data-show_dislike_label="true" data-counter_zero_show="true" data-i18n_dislike="No"></span>
<script>(function(d,e,s){if(d.getElementById("likebtn_wjs"))return;a=d.createElement(e);m=d.getElementsByTagName(e)[0];a.async=1;a.id="likebtn_wjs";a.src=s;m.parentNode.insertBefore(a, m)})(document,"script","//w.likebtn.com/js/w/widget.js");</script>
<!-- LikeBtn.com END -->

{% include docs-footer.md %}