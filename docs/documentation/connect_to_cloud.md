---
layout: default
title: Connect to Soda Cloud
parent: Documentation
nav_order: 11
---

# Connect to Soda Cloud

To use the **[Soda Cloud]({% link documentation/glossary.md %}#soda-cloud)** web user interface to monitor your data, you must install and configure the **[Soda SQL]({% link documentation/glossary.md %}#soda-sql)** command line tool, then connect it to your Soda Cloud account.  

Soda SQL uses an API to connect to Soda Cloud. To use the API, you must generate API keys in your Soda Cloud account, then add them to the [warehouse YAML]({% link documentation/warehouse.md %}) file that Soda SQL created. 


1. If you have not already done so, create a Soda Cloud account at [cloud.soda.io](https://cloud.soda.io/signup).
2. Use the instructions in [Install Soda SQL]({% link getting-started/installation.md %}) to install Soda SQL.
3. Follow steps in the [Quick start tutorial]({% link getting-started/5_min_tutorial.md %}) to create your warehouse YAML file, connect to your [warehouse]({% link documentation/glossary.md %}#warehouse), analyze your [tables]({% link documentation/glossary.md %}#table), and run a [scan]({% link documentation/glossary.md %}#scan) on the data.
4. Open the `warehouse.yml` file in a text editor, then add the following to the file:
```shell
soda_account:
  host: cloud.soda.io
  api_key_id: env_var(API_PUBLIC)
  api_key_secret: env_var(API_PRIVATE)
```
5. Save the `warehouse.yml` file.
6. Open your `~/.soda/env_vars.yml` file in a text editor, then add the following to the file:
```shell
[warehouse_name]:
  ...
  API_PUBLIC: 
  API_PRIVATE: 
```
7. In Soda Cloud, navigate to your **Profile** page to generate new API keys. 
    * Copy the Public key, then paste it into the `env_vars.yml` file as the value for `API_PUBLIC`.
    * Copy the Private key, then paste it into the `env_vars.yml` file as the value for `API_PRIVATE`.
8. Save the changes to the `env_vars.yml` file. Close the API Keys create dialog box in your Soda Cloud account.
9. From the command line, use Soda SQL to scan the tables in your warehouse again.
```shell
$ soda scan warehouse.yml tables/[dbtablename].yml
```
10. Navigate to your Soda Cloud account and refresh the browser. Review the results of your scan in Monitor Results.

## Go further

* Learn how to [create monitors and alerts]({% link documentation/monitors.md %}).
* Learn more about the [anatomy of a scan]({% link documentation/scan.md %}).
* Learn more about the [warehouse yaml]({% link documentation/warehouse.md %}) file.
* [Contact us](https://github.com/sodadata/Soda SQL/discussions) with a question or comment.
