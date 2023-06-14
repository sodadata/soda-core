---
layout: default
title: Migrate from Soda SQL to Soda Core
description: 
parent: Soda Core
---

# Migrate from Soda SQL to Soda Core 
<!--Linked to UI, access Shlink-->
*Last modified on {% last_modified_at %}*

Soda SQL, the original command-line tool that Soda created to test for data quality, has been deprecated and replaced by [Soda Core]({% link soda-core/overview-main.md %}). As the primary tool for data reliability, you can install Soda Core, connect to your data sources, and convert your Soda SQL tests into SodaCL checks. 

Use the following guide to understand the migration path from Soda SQL to Soda Core. <br />Contact <a href="mailto:support@soda.io">support@soda.io</a> for help migrating from Soda SQL to Soda Core. 

[Migrate to Soda Core](#migrate-to-soda-core)<br />
[(Optional) Adjust in Soda Cloud](#optional-adjust-in-soda-cloud)<br />
[Migration implications](#migration-implications)<br />
<br />
<br />

## Migrate to Soda Core

1. [Install]({% link soda-core/installation.md %}) Soda Core.
2. [Configure]({% link soda-core/configuration.md %}) Soda Core to connect to your data sources.
3. Convert your Soda SQL tests in your scan YAML file to SodaCL checks in a checks YAML file. Refer to [SodaCL documentation]({% link soda-cl/metrics-and-checks.md %}) and some [example conversions](#example-conversions-from-tests-to-checks), below.<br />
4. Use Soda Core to [run fresh scans]({% link soda-core/scan-core.md %}) of data in your data sources.
5. (Optional) Revise your [programmatic scans]({% link soda-core/programmatic.md %}) to use Soda Core.

### Example conversions from tests to checks

```yaml
# SODA SQL - change over time
# Requires a Soda Cloud account
table_name: orders.yaml
metrics
  - row_count
  - missing_count
  - missing_percentage
  - ...
columns:
  ID:
    metrics:
      - distinct
      - duplicate_count
      - valid_count
      - avg
    historic metrics:
      - name: avg_dup_7
        type: avg
        metric: duplicate_count
        count: 7
    tests:
      - duplicate_count < avg_dup_7

# SODA CORE - change over time
# Requires a Soda Cloud account
checks for orders:
  - change avg last 7 for duplicate_count < 50
```

<br />

```yaml
# SODA SQL - invalid
table_name: fulfillment
metrics:
  - row_count
  - missing_count
  - missing_percentage
  - values_count
  - ...
columns:
  country:
    valid_values:
      - US
      - UK
      - CN
    tests:
      - invalid_percentage == 0

# SODA CORE - invalid
checks for fulfillment:
  - invalid_percent(country) = 0%:
      valid values: [US, UK, CN]
```

<br />


## (Optional) Adjust in Soda Cloud

If you connected Soda SQL to a Soda Cloud account, you can continue to use the same Soda Cloud account with Soda Core, with some [implications](#migration-implications).

1. [Connect]({% link soda-core/connect-core-to-cloud.md%}) Soda Core to your existing Soda Cloud account.
2. Prepare your team to review **Check Results** in lieu of **Monitor Results**. Checks replace the concept of Monitors in Soda Cloud. 
3. Write new checks in Soda Cloud in the context of [Soda Agreements]({% link soda-cloud/agreements.md%}).
4. (Optional) Add a new data source in Soda Cloud using a [Soda Agent]({% link soda-agent/basics.md%}).

## Migration implications

There are a couple of implications to take into consideration when migrating from Soda SQL to Soda Core.
* The historical test result data accumulated in Soda Cloud for a Soda SQL dataset does not carry over to the same dataset that you add using Soda Core. In other words, all previously recorded historical test results do not influence new scan result data in Soda Cloud. Soda SQL and Soda Core results remain forever separate from each other.
* Any Monitors that you or your team created in Soda Cloud do not apply to the same datasets that you add using Soda Core. You must convert the monitors into SodaCL checks within the context of an [Agreement]({% link soda-cloud/agreements.md %}). 



## Go further

* Contact <a href="mailto:support@soda.io">support@soda.io</a> for help migrating from Soda SQL to Soda Core.
* Need help? Join the <a href="https://community.soda.io/slack" target="_blank"> Soda community on Slack</a>.
* Learn more about [writing checks with SodaCL]({% link soda/quick-start-sodacl.md %}).
<br />

---

Was this documentation helpful?

<!-- LikeBtn.com BEGIN -->
<span class="likebtn-wrapper" data-theme="tick" data-i18n_like="Yes" data-ef_voting="grow" data-show_dislike_label="true" data-counter_zero_show="true" data-i18n_dislike="No"></span>
<script>(function(d,e,s){if(d.getElementById("likebtn_wjs"))return;a=d.createElement(e);m=d.getElementsByTagName(e)[0];a.async=1;a.id="likebtn_wjs";a.src=s;m.parentNode.insertBefore(a, m)})(document,"script","//w.likebtn.com/js/w/widget.js");</script>
<!-- LikeBtn.com END -->

{% include docs-footer.md %}