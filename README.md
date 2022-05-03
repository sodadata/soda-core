<p align="center"><img src="https://raw.githubusercontent.com/sodadata/docs/main/assets/images/soda-banner.png" alt="Soda logo" /></p>

<h1 align="center">Soda Core</h1>
<p align="center"><b>Data testing, monitoring and profiling for SQL-accessible data.</b></p>

<p align="center">
  <a href="https://github.com/sodadata/soda-core/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
</p>


Soda Core is a free, open-source, command-line tool that enables you to use the Soda Checks Language to turn user-defined input into aggregated SQL queries. 

When it runs a scan on a dataset, Soda Core executes the checks to find invalid, missing, or unexpected data. When your Soda Checks fail, they surface the data that you defined as “bad”.

Connect Soda Core to your data source, then define your Soda Checks for data quality in a checks.yml file. Use Soda Core to run scans of your data to execute the checks you defined. 

## Get started

Soda Core currently supports PostgreSQL, Amazon Redshift, GCP BigQuery, and Snowflake. To get started, use the install command, replacing `soda-postgres` with the package that matches your data source. 

`pip install soda-postgres`

* `soda-core-postgres`
* `soda-core-redshift`
* `soda-core-bigquery`
* `soda-core-snowflake`



## Documentation

* [Soda Core](https://docs.soda.io/soda-core/overview.html)
* [Soda Checks Language (SodaCL)](https://docs.soda.io/soda-cl/soda-cl-overview.html)

