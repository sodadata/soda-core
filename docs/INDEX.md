---
layout: default
title: Get Started
nav_order: 1
has_children: true
permalink: /
---

<p align="center"><img src="https://raw.githubusercontent.com/sodadata/soda-sql/main/docs/assets/images/soda-banner.png" alt="Soda logo" /></p>


<p align="center"><img src="https://raw.githubusercontent.com/sodadata/soda-sql/main/docs/assets/images/soda-sql-index.png" alt="Soda SQL logo" width="700" height="700"/></p>

**Soda SQL** is an open-source command-line tool. It utilizes user-defined input to prepare SQL queries that run tests on tables in a database to find invalid, missing, or unexpected data. When tests fail, they surface "bad" data that you and your data engineering team can fix, ensuring downstream analysts are using "good" data to make decisions.

{% include compatible-warehouses.md %}


* [Install Soda SQL]({% link getting-started/installation.md %}) from your command-line interface.
* Follow the [Quick start tutorial]({% link getting-started/5_min_tutorial.md %}) to set up Soda SQL and run your first scan in minutes.
* Learn the [Basics of Soda SQL]({% link documentation/concepts.md %}#soda-sql-basics).
* Use Soda SQL with your [data orchestration tool]({% link documentation/orchestrate_scans.md %}) to automate data monitoring and testing.
* Use Soda SQL as a stand-alone solution, or [connect to a free Soda Cloud account]({% link documentation/connect_to_cloud.md %}) to use the web app to monitor data quality.
* Help us make Soda SQL even better! Join our [developer community]({% link community.md %}) and [contribute](https://github.com/sodadata/soda-sql/blob/main/CONTRIBUTING.md).

<br />

<p align="center"><img src="https://raw.githubusercontent.com/sodadata/soda-sql/main/docs/assets/images/soda-cloud-index.png" alt="Soda Cloud logo" width="700" height="700"/></p>

Connect Soda SQL to a free **Soda Cloud** account where you and your team can use the web application to monitor test results and collaborate to keep your data issue-free. 

* Set up your free Soda Cloud account at [cloud.soda.io](https://cloud.soda.io/signup).
* Soda SQL can run without Soda Cloud, but Soda Cloud Free and Teams editions depend upon Soda SQL! Install Soda SQL, then [connect it to your Soda Cloud Account]({% link documentation/connect_to_cloud.md %}).
* Learn more about [Soda Cloud architecture]({% link documentation/soda-cloud-architecture.md %}).
* [Create monitors and alerts]({% link documentation/monitors.md %}) to notify your team about data issues.

