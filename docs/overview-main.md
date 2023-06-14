---
layout: default
title: Soda Core overview
description: Soda Core is an open-source, CLI tool that enables you to use the Soda Checks Language to turn user-defined input into SQL queries.
parent: Soda Core
redirect_from: 
- /soda-core/
- /soda-core/overview.html
- /soda-cl/soda-core-overview.html
---
<br />

![soda-core-logo](/assets/images/soda-core-logo.png){:height="230px" width="230px"} 
<br />
<br />

<!--Linked to UI, access Shlink-->

&#10004;  An open-source, CLI tool and Python library for data reliability<br /> <br />
&#10004;  Compatible with [Soda Checks Language (SodaCL)]({% link soda-cl/soda-cl-overview.md %}) and [Soda Cloud]({% link soda-cloud/overview.md %}) <br /> <br />
&#10004;  Enables data quality testing both in and out of your [data pipeline]({% link soda-core/orchestrate-scans.md %}), for data observability and reliability <br /> <br />
&#10004;  Enables [programmatic scans]({% link soda-core/programmatic.md %}) on a time-based schedule <br /> <br />
<br />

#### Example checks
{% include code-header.html %}
```yaml
# Checks for basic validations
checks for dim_customer:
  - row_count between 10 and 1000
  - missing_count(birth_date) = 0
  - invalid_percent(phone) < 1 %:
      valid format: phone number
  - invalid_count(number_cars_owned) = 0:
      valid min: 1
      valid max: 6
  - duplicate_count(phone) = 0
checks for dim_product:
  - avg(safety_stock_level) > 50
```
{% include code-header.html %}
```yaml
# Check for schema changes
checks for dim_product:
  - schema:
      name: Find forbidden, missing, or wrong type
      warn:
        when required column missing: [dealer_price, list_price]
        when forbidden column present: [credit_card]
        when wrong column type:
          standard_cost: money
      fail:
        when forbidden column present: [pii*]
        when wrong column index:
          model_name: 22
```
{% include code-header.html %}
```yaml
# Check for freshness 
checks for dim_product:
  - freshness(start_date) < 1d
```
{% include code-header.html %}
```yaml
# Check for referential integrity
checks for dim_department_group:
  - values in (department_group_name) must exist in dim_employee (department_name)
```
<br />

<div class="docs-html-content">
    <section class="docs-section" style="padding-top:0">
        <div class="docs-section-row">
            <div class="docs-grid-3cols">
                <div>
                    <img src="/assets/images/icons/icon-pacman@2x.png" width="54" height="40">
                    <h2>Get started</h2>
                    <a href="https://docs.soda.io/soda-core/installation.html" target="_blank">Download and install Soda Core </a> 
                    <a href="https://docs.soda.io/soda-core/configuration.html" target="_blank">Prepare a configuration.yml file</a>
                    <a href="https://docs.soda.io/soda/quick-start-sodacl.html" target="_blank">Write checks in a checks.yml file</a>
                    <a href="https://docs.soda.io/soda-core/scan-core.html" target="_blank">Run a scan</a>
                </div>
            </div>
        </div>        
    </section>
</div>

## Why Soda Core?

Simplify the work of maintaining healthy data and eliminate the bottlenecks in data quality management.
* Download a free, OSS CLI tool and configure settings and data quality checks in two simple YAML files to start scanning your data within minutes.
* Connect Soda Core to over a dozen data sources to scan volumes of data for quality.
* Use the Soda Core Python library to build programmatic scans that you can use in conjunction with orchestration tools like Airflow or Prefect to automate pipeline actions when data quality fails.
* Write data quality checks using SodaCL, a low-code, human-readable, domain-specific language for data quality management.
* Run the same scans for data quality in multiple environments such as development, staging, and production.
* Connect to Soda Cloud to unlock historic metric storage, data quality incident tracking, and change-over-time metrics.

---

Was this documentation helpful?

<!-- LikeBtn.com BEGIN -->
<span class="likebtn-wrapper" data-theme="tick" data-i18n_like="Yes" data-ef_voting="grow" data-show_dislike_label="true" data-counter_zero_show="true" data-i18n_dislike="No"></span>
<script>(function(d,e,s){if(d.getElementById("likebtn_wjs"))return;a=d.createElement(e);m=d.getElementsByTagName(e)[0];a.async=1;a.id="likebtn_wjs";a.src=s;m.parentNode.insertBefore(a, m)})(document,"script","//w.likebtn.com/js/w/widget.js");</script>
<!-- LikeBtn.com END -->

{% include docs-footer.md %}