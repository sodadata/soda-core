---
layout: default
title: Soda Cloud architecture
parent: Documentation
nav_order: 12
---

# Soda Cloud architecture

![scan-anatomy](../assets/images/soda-cloud-arch.png){:height="550px" width="550px"}

**[Soda Cloud]({% link documentation/glossary.md %}#soda-cloud)** and **[Soda SQL]({% link documentation/glossary.md %}#soda-sql)** work together to help you monitor your data and alert you when there is a data quality issue. 

Installed in your environment, you use the Soda SQL command-line tool to [scan]({% link documentation/glossary.md %}#scan) data in your [warehouses]({% link documentation/glossary.md %}#warehouse). Soda SQL uses a secure API to connect to Soda Cloud. When it completes a scan, it pushes the scan results to your Soda Cloud account where you can log in and examine the details in the web application. Notably, Soda SQL only ever pushes *metadata* to Soda Cloud; all your data stays inside your private network.

When you create a [monitor]({% link documentation/glossary.md %}#monitor) in Soda Cloud's web application, Soda SQL uses the monitor settings to add new [tests]({% link documentation/glossary.md %}#test) when it runs a scan on data in a specific warehouse. A monitor is essentially a way to create Soda SQL tests using the web application instead of adjusting [scan YAML file]({% link documentation/glossary.md %}#scan-yaml) contents directly in your Soda project directory.

## Go further

* Learn more about [How Soda SQL works]({% link documentation/concepts.md %}).
* Learn more about [Soda SQL scans]({% link documentation/scan.md %}).
* Learn more about [Soda SQL tests]({% link documentation/tests.md %}) and [Soda Cloud monitors and alerts]({% link documentation/monitors.md %}).
* [Connect Soda SQL to Soda Cloud]({% link documentation/connect_to_cloud.md %}).