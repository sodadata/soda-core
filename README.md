
<h1 align="center">Soda Core</h1>
<p align="center"><b>Data quality testing for SQL-, Spark-, and Pandas-accessible data.</b></p>

<p align="center">
  <a href="https://github.com/sodadata/soda-core/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="#"><img src="https://static.pepy.tech/personalized-badge/soda-core?period=total&units=international_system&left_color=black&right_color=green&left_text=Downloads"></a>
</p>

### Upcoming Major Release: Soda Core 4.0 (January 28)

> We’re preparing a major release of Soda Core (v4.0), scheduled for January 28.
> 
> This release introduces **Data Contracts as the default way to define data quality rules for tables**.
>
> ⚠️ This is a breaking change: Soda Core is moving from the checks language to a Data Contracts–based syntax.
> 
> The new approach offers a cleaner, more structured, and more maintainable way to define and manage data quality rules, based on community feedback and real-world usage.
> 
> If you are currently using Soda Core, you will need to migrate your existing checks to Data Contracts when upgrading to v4.
> 
> 📖 **More details**, including migration guidance and examples of the new Data Contract format, will be shared closer to the release date.
>
> 📄 Check out the [Soda Core 4.0 documentation](https://docs.soda.io/soda-v4/deployment-options/soda-python-libraries) to learn how to install it and see what’s coming.
> ### Soda Cloud users
> If you are using [Soda Cloud](https://soda.io/?utm_source=github&utm_medium=readme&utm_campaign=soda_v4_release&utm_content=soda_cloud), a Customer Engineer will reach out to you to help schedule and support your migration to v4.
> ### Not ready to upgrade yet?
> If you want to stay on Soda Core v3 and avoid automatically upgrading to v4, make sure to pin your dependency to a v3 version. Check out all previous versions of [Soda Core releases](https://github.com/sodadata/soda-core/releases).
> #### Example (pip)
> Specify a v3 version to prevent automatic upgrades:
> ```bash
> pip install soda-core==3.5.6
> ```
> We strongly recommend pinning versions in production environments to avoid unexpected breaking changes.


<hr/>

&#10004;  An open-source, CLI tool and Python library for data quality testing<br />
&#10004;  Compatible with the <a href="https://docs.soda.io/soda-cl/soda-cl-overview.html" target="_blank">Soda Checks Language (SodaCL)</a>  <br />
&#10004;  Enables data quality testing both in and out of your data pipelines and development workflows<br />
&#10004;  Integrated to allow a Soda scan in a data pipeline, or programmatic scans on a time-based schedule <br />


Soda Core is a free, open-source, command-line tool and Python library that enables you to use the Soda Checks Language to turn user-defined input into aggregated SQL queries. 

When it runs a scan on a dataset, Soda Core executes the checks to find invalid, missing, or unexpected data. When your Soda Checks fail, they surface the data that you defined as bad-quality.

#### Soda Library 

Consider migrating to **[Soda Library](https://docs.soda.io/soda/quick-start-sip.html)**, an extension of Soda Core that offers more features and functionality, and enables you to connect to a [Soda Cloud](https://docs.soda.io/soda-cloud/overview.html) account to collaborate with your team on data quality.
* Use [Group by](https://docs.soda.io/soda-cl/group-by.html) and [Group Evolution](https://docs.soda.io/soda-cl/group-evolution.html) configurations to intelligently group check results
* Leverage [Reconciliation checks](https://docs.soda.io/soda-cl/recon.html) to compare data between data sources for data migration projects.
* Use [Schema Evolution](https://docs.soda.io/soda-cl/schema.html#define-schema-evolution-checks) checks to automatically validate schemas.
* Set up [Anomaly Detection](https://docs.soda.io/soda-cl/anomaly-detection.html) checks to automatically learn patterns and discover anomalies in your data.

[Install Soda Library](https://docs.soda.io/soda-library/install.html) and get started with a 45-day free trial.

<br />

## Get started

This repository hosts the open source Soda Core packages which are installable using the **Public PyPI installation flow** described in [Soda's documentation](https://docs.soda.io/soda-v4/deployment-options/soda-python-libraries#public-pypi-installation-flow) 

### Requirements
To use Soda, you must have installed the following on your system.

* **Python 3.8, 3.9, 3.10 or 3.11.** <br>
To check your existing version, use the CLI command: `python --version or python3 --version`. If you have not already installed Python, consider using `pyenv` to manage multiple versions of Python in your environment.

* **Pip 21.0 or greater.**
To check your existing version, use the CLI command: pip --version

* Optionally, **a Soda Cloud account**; see [how to sign up](https://docs.soda.io/soda-v4/quickstart#sign-up).

Best practice dictates that you install the Soda CLI using a virtual environment. If you haven't yet, in your command-line interface tool, create a virtual environment in the .venv directory using the commands below. Depending on your version of Python, you may need to replace python with python3 in the first command.


Copy
python -m venv .venv
source .venv/bin/activate


### Public PyPI installation flow
    To use the open source Soda Core python packages, you must install them from the public Soda PyPi registry: https://pypi.cloud.soda.io/simple .

Install the Soda Core package for your data source. This gives you access to all the basic CLI functionality for working with contracts.

```
pip install -i https://pypi.cloud.soda.io/simple --pre -U "soda-postgres>4"
```
Replace `soda-postgres` with the appropriate package for your data source. See the [Data source reference for Soda Core](https://docs.soda.io/soda-v4/reference/data-source-reference-for-soda-core) for supported packages and configurations.
