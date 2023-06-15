# Soda Core usage statistics

To understand how users are using Soda Core, and to proactively capture bugs and performance issues, the Soda development team has added telemetry event tracking to Soda Core. 

Soda tracks usage statistics using the Open Telemetry Framework. The data Soda tracks is completely anonymous, does not contain any personally identifiying information (PII) in any form, and is purely for internal use.

## Opt out of usage statistics

Soda Core collects usage statistics by default. You can opt-out from sending Soda Core usage statistics at any time by adding the following to your `~/.soda/config.yml` or `.soda/config.yml` file:

```yaml
send_anonymous_usage_stats: false
```