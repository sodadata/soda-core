---
layout: default
title: Orchestrate scans
parent: Documentation
nav_order: 9
---

# Orchestrate scans

This section explains how to run scans as part of your data pipeline and
stop the pipeline when necessary to prevent bad data flowing downstream.

Soda SQL is build in such a way that it's easy to run it as a step in your
pipeline orchestration.

Use your orchestration tool to configure if the soda scan should be blocking the pipeline
(for testing) or run in parallel (for monitoring).

## Airflow

TODO: describe how to run Soda scans in Airflow.

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions/new?title=Contributing%20Airflow) to let
others know you're starting on this.

## Other orchestration solutions

TODO: describe how to run Soda scans in orchestration tools like

* AWS Glue
* Prefect
* Dagster
* Fivetran
* Matillion
* Luigi

If you're reading this and thinking: "I want to contribute!" Great.
[Post an note on GitHub](https://github.com/sodadata/soda-sql/discussions/new) to let others know
you're starting on this.