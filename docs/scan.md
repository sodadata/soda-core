# Scan

This section explains the yml structure of scan.yml files.

For running scans, see either [the CLI](cli.md) or [Orchestrate scans](orchestrate_scans.md) 

## Top level scan.yml keys

In a `scan.yaml` file, you configure which metrics should be computed and 
which tests should be checked.

Top level configuration keys:

| Key | Description | Required |
| --- | ----------- | -------- |
| table_name | The table name. TODO add if case matters. | Required |
| metrics | The list of metrics to compute. Column metrics specified here will be computed on each column. |
| columns |  |
| mins_maxs_limit |  |
| frequent_values_limit |  |
| sample_percentage |  |
| sample_method |  |

## Metrics

### Table metrics

| Meric | Description |
| ----- | ------------|
| row_count |  |
| schema |  |

#3# Column metrics

| Meric | Description |
| ----- | ------------|
| missing_count |  |
| missing_percentage |  |
| values_count |  |
| values_percentage |  |
| valid_count |  |
| valid_percentage |  |
| invalid_count |  |
| invalid_percentage |  |
| min |  |
| max |  |
| avg |  |
| sum |  |
| variance |  |
| stddev |  |
| min_length |  |
| max_length |  |
| avg_length |  |
| distinct |  |
| unique_count |  |
| duplicate_count |  |
| uniqueness |  |
| maxs |  |
| mins |  |
| frequent_values |  |
| histogram |  |

### Metric categories

| Meric category | Metrics |
| -------------- | ------------|
| missing | missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage |
| validity | valid_count<br/>valid_percentage<br/>invalid_count<br/>invalid_percentage |
| duplicates | distinct<br/>unique_count<br/>uniqueness<br/>duplicate_count |

### Implied metrics

| Any metric in | Implies metrics |
| ------------- | --------------- |
| valid_count<br/>valid_percentage<br/>invalid_count<br/>invalid_percentage | missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage |
| missing_count<br/>missing_percentage<br/>values_count<br/>values_percentage | row_count |
| histogram | min<br/>max |

## Columns

Column configuration keys:

| Key | Description |
| --- | ----------- |
| metrics |  |
| tests |  |
| missing_values |  |
| missing_format |  |
| missing_regex |  |
| valid_format |  |
| valid_regex |  |
| valid_values |  |
| valid_min |  |
| valid_max |  |
| valid_min_length |  |
| valid_max_length |  |
