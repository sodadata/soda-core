---
layout: default
title: Soda Core usage statistics
description: To understand how users are using Soda Core, the Soda dev team added telemetry event tracking to Soda Core. See instructions to opt-out.
parent: Reference
---

# Soda Core usage statistics
*Last modified on {% last_modified_at %}*

To understand how users are using Soda Core, and to proactively capture bugs and performance issues, the Soda development team has added telemetry event tracking to Soda Core. 

Soda tracks usage statistics using the Open Telemetry Framework. The data Soda tracks is completely anonymous, does not contain any personally identifiying information (PII) in any form, and is purely for internal use.

## Opt out of usage statistics

Soda Core collects usage statistics by default. You can opt-out from sending Soda Core usage statistics at any time by adding the following to your `~/.soda/config.yml` or `.soda/config.yml` file:
{% include code-header.html %}
```yaml
send_anonymous_usage_stats: false
```

## Go further

* Learn [How Soda Core works]({% link soda-core/how-core-works.md %}).
* Need help? Join the <a href="https://community.soda.io/slack" target="_blank"> Soda community on Slack</a>.

<br />

---

Was this documentation helpful?

<!-- LikeBtn.com BEGIN -->
<span class="likebtn-wrapper" data-theme="tick" data-i18n_like="Yes" data-ef_voting="grow" data-show_dislike_label="true" data-counter_zero_show="true" data-i18n_dislike="No"></span>
<script>(function(d,e,s){if(d.getElementById("likebtn_wjs"))return;a=d.createElement(e);m=d.getElementsByTagName(e)[0];a.async=1;a.id="likebtn_wjs";a.src=s;m.parentNode.insertBefore(a, m)})(document,"script","//w.likebtn.com/js/w/widget.js");</script>
<!-- LikeBtn.com END -->

{% include docs-footer.md %}