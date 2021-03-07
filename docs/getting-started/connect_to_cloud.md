---
layout: default
title: Connect to Soda Cloud
parent: Getting Started
nav_order: 3
---

# Connect to Soda Cloud

To use the Soda Cloud web user interface to monitor your data, you must install and configure the soda-sql command line tool, then connect it to your Soda Cloud account.

* The **soda-sql** command line tool does the work of scanning the data in your database. 
* Your **Soda Cloud** account is where soda-sql pushes the results of its scans. Use the web UI to examine the results and set up alerts.  

soda-sql uses an API to connect to Soda Cloud. To use the API, you must generate API keys in your Soda Cloud account, then add them to the warehouse yaml file that soda-sql created. 



1. If you have not already done so, create a Soda Cloud account at [cloud.soda.io](https://cloud.soda.io/signup).
2. Use the instructions in the [Installation guide]({% link getting-started/installation.md %}) to install soda-sql.
3. Follow steps 3 - 6 in the [5 min tutorial]({% link getting-started/5_min_tutorial.md %}) to create your warehouse yaml file, connect to your database, analyze your database tables, and run a scan on the data.
4. Open the `warehouse.yml` file in a text editor, then add the following to the file:
```shell
soda_account:
  host: cloud.soda.io
  api_key_id: 
  api_key_secret:  
```
5. In Soda Cloud, navigate to your Profile page to generate new API keys. 
    * Copy the Public key, then paste it into the `warehouse.yml` file as the value for `api_key_id'.
    * Copy the Private key, then paste it into the `warehouse.yml` file as the value for `api_key_secret'.
6. Save the changes to the `warehouse.yml` file.
7. Use soda-sql to scan the tables in your warehouse again.
```shell
soda scan warehouse.yml tables/[dbtablename].yml
```
9. Navigate to your Soda Cloud account and refresh the browser. Review the results of your scan in Monitor Results.

## Next

* Learn more about the [anatomy of a scan]({% link documentation/scan.md %}).
* Learn more about the [warehouse yaml]({% link documentation/warehouse.md %}) file.
* [Contact us](https://github.com/sodadata/soda-sql/discussions) with a question or comment.