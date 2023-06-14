---
layout: default
title: Install Soda Core
description: From your command-line interface, execute a pip install command to install Soda Core.
parent: Soda Core
redirect_from: 
- /soda-core/install-scientific.html
- /soda-core/get-started.html
---

# Install Soda Core 
*Last modified on {% last_modified_at %}*

Soda Core is a command-line interface (CLI) tool that enables you to scan the data in your data source to surface invalid, missing, or unexpected data. Alternatively, you can use the Soda Core Python library to programmatically execute scans; see [Define programmatic scans using Python]({% link soda-core/programmatic.md %}).
<br />

[Compatibility](#compatibility)<br />
[Requirements](#requirements)<br />
[Install](#install)<br />
[Upgrade](#upgrade)<br />
[Install Soda Core Scientific](#install-soda-core-scientific)<br />
[Go further](#go-further)<br />

## Compatibility

Use Soda Core to scan a variety of data sources.<br />

{% include compatible-datasources.md %}

## Requirements

To use Soda Core, you must have installed the following on your system.

* Python 3.8 or greater. To check your existing version, use the CLI command: `python --version` or `python3 --version` <br /> 
If you have not already installed Python, consider using <a href="https://github.com/pyenv/pyenv/wiki" target="_blank">pyenv</a> to manage multiple versions of Python in your environment.
* Pip 21.0 or greater. To check your existing version, use the CLI command: `pip --version`

## Install

<div class="warpper">
  <input class="radio" id="one" name="group" type="radio" checked>
  <input class="radio" id="two" name="group" type="radio">
  <input class="radio" id="three" name="group" type="radio">
  <div class="tabs">
  <label class="tab" id="one-tab" for="one">MacOS, Linux</label>
  <label class="tab" id="two-tab" for="two">Windows</label>
  <label class="tab" id="three-tab" for="three">Docker</label>
    </div>
  <div class="panels">
  <div class="panel" id="one-panel" markdown="1">

1. Best practice dictates that you install the Soda Core CLI using a virtual environment. In your command-line interface tool, create a virtual environment in the `.venv` directory using the commands below. Depending on your version of Python, you may need to replace `python` with `python3` in the first command.
```shell
python -m venv .venv
source .venv/bin/activate
```
2. Upgrade pip inside your new virtual environment.
```shell
pip install --upgrade pip
```
3. Execute the following command, replacing `soda-core-postgres` with the install package that matches the type of data source you use to store data.
```shell
pip install soda-core-postgres
```

| Data source | Install package | 
| ----------- | --------------- | 
| Amazon Athena | `soda-core-athena` |
| Amazon Redshift | `soda-core-redshift` | 
| Apache Spark DataFrames <br /> (For use with [programmatic Soda scans]({% link soda-core/programmatic.md %}), only.) | `soda-core-spark-df` |
| Azure Synapse (Experimental) | `soda-core-sqlserver` |
| ClickHouse (Experimental) | `soda-core-mysql` |
| Dask and Pandas (Experimental)  | `soda-core-pandas-dask` |
| Databricks  | `soda-core-spark[databricks]` |
| Denodo (Experimental) | `soda-core-denodo` |
| Dremio | `soda-core-dremio` | 
| DuckDB (Experimental)  | `soda-core-duckdb` |
| GCP Big Query | `soda-core-bigquery` | 
| IBM DB2 | `soda-core-db2` |
| Local file | Use Dask. |
| MS SQL Server | `soda-core-sqlserver` |
| MySQL | `soda-core-mysql` |
| OracleDB | `soda-core-oracle` |
| PostgreSQL | `soda-core-postgres` |
| Snowflake | `soda-core-snowflake` | 
| Trino | `soda-core-trino` |
| Vertica (Experimental) | `soda-core-vertica` |


To deactivate the virtual environment, use the following command:
```shell
deactivate
```


  </div>
  <div class="panel" id="two-panel" markdown="1">

1. Best practice dictates that you install the Soda Core CLI using a virtual environment. In your command-line interface tool, create a virtual environment in the `.venv` directory using the commands below. Depending on your version of Python, you may need to replace `python` with `python3` in the first command. Reference the <a href="https://virtualenv.pypa.io/en/legacy/userguide.html#activate-script" target="_blank">virtualenv documentation</a> for activating a Windows script.
```shell
python -m venv .venv
.venv\Scripts\activate
```
2. Upgrade pip inside your new virtual environment.
```shell
pip install --upgrade pip
```
3. Execute the following command, replacing `soda-core-postgres` with the install package that matches the type of data source you use to store data.
```shell
pip install soda-core-postgres
```

| Data source | Install package | 
| ----------- | --------------- | 
| Amazon Athena | `soda-core-athena` |
| Amazon Redshift | `soda-core-redshift` | 
| Apache Spark DataFrame <br /> (For use with [programmatic Soda scans]({% link soda-core/programmatic.md %}), only.) | `soda-core-spark-df` |
| Azure Synapse (Experimental) | `soda-core-sqlserver` |
| ClickHouse (Experimental) | `soda-core-mysql` |
| Dask and Pandas (Experimental)  | `soda-core-pandas-dask` |
| Databricks  | `soda-core-spark[databricks]` |
| Denodo (Experimental) | `soda-core-denodo` |
| Dremio | `soda-core-dremio` | 
| DuckDB (Experimental) | `soda-core-duckdb` |
| GCP Big Query | `soda-core-bigquery` | 
| IBM DB2 | `soda-core-db2` |
| MS SQL Server | `soda-core-sqlserver` |
| MySQL | `soda-core-mysql` |
| OracleDB | `soda-core-oracle` |
| PostgreSQL | `soda-core-postgres` |
| Snowflake | `soda-core-snowflake` | 
| Trino | `soda-core-trino` |
| Vertica (Experimental) | `soda-core-vertica` |

To deactivate the virtual environment, use the following command:
```shell
deactivate
```

Reference the <a href="https://virtualenv.pypa.io/en/legacy/userguide.html#activate-script" target="_blank">virtualenv documentation</a> for activating a Windows script.


  </div>
  <div class="panel" id="three-panel" markdown="1">

{% include docker-soda-core.md %}

  </div>
  </div>
</div>

<br />

## Upgrade

To upgrade your existing Soda Core tool to the latest version, use the following command, replacing `soda-core-redshift` with the install package that matches the type of data source you are using.
```shell
pip install soda-core-redshift -U
```

## Install Soda Core Scientific

Install Soda Core Scientific to be able to use SodaCL [distribution checks]({% link soda-cl/distribution.md %}) or [anomaly score checks]({% link soda-cl/anomaly-score.md %}).

You have three installation options to choose from:
* [Install Soda Core Scientific in a virtual environment (Recommended)](#install-soda-core-scientific-in-a-virtual-environment-recommended)
* [Use Docker to run Soda Core with Soda Scientific](#use-docker-to-run-soda-core-scientific)

### Install Soda Core Scientific in a virtual environment (Recommended)

{% include install-soda-scientific.md %}

<br />

#### Error: Library not loaded

{% include troubleshoot-anomaly-check-tbb.md %}

### Use Docker to run Soda Core Scientific

{% include docker-soda-core.md %}

## Go further

* Next: [Configure]({% link soda-core/configuration.md %}) your newly-installed Soda Core to connect to your data source.
* Need help? Join the <a href="https://community.soda.io/slack" target="_blank"> Soda community on Slack</a>.
<br />

---

Was this documentation helpful?

<!-- LikeBtn.com BEGIN -->
<span class="likebtn-wrapper" data-theme="tick" data-i18n_like="Yes" data-ef_voting="grow" data-show_dislike_label="true" data-counter_zero_show="true" data-i18n_dislike="No"></span>
<script>(function(d,e,s){if(d.getElementById("likebtn_wjs"))return;a=d.createElement(e);m=d.getElementsByTagName(e)[0];a.async=1;a.id="likebtn_wjs";a.src=s;m.parentNode.insertBefore(a, m)})(document,"script","//w.likebtn.com/js/w/widget.js");</script>
<!-- LikeBtn.com END -->

{% include docs-footer.md %}
