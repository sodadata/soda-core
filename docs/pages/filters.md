# Filters

### Dataset filter

Use `filter` to apply the checks to a filtered set of records in the dataset.

The `filter` is a SQL expression that is appended to the WHERE-clause of the 
generated queries.  It's used mostly to filter the slice of data tested with the contract 
for the latest time partition or for a particular sub category of the data.

### Building a time partition filter

The following example shows how a time partition filter for 'today' based on the `NOW` variable 
using postgres SQL functions.  The `NOW` variable is automatically initialized by the contract 
verification.

The postgres functions `date_trunc` is used to compute the previous midnight.  And the 
`+ interval '1 day'` is used to compute 24 hours later than the start timestamp.

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

filter: |
    date_trunc('day', timestamp '${var.NOW}') < {column_name_quoted}
    AND {column_name_quoted} <= date_trunc('day', timestamp '${var.NOW}') + interval '1 day'

columns:
  - name: country
    checks:
      - missing:
```

### Reusing the time partition filter

> Note: failed_rows check type is not yet supported

Use a variable to leverage the checks filter also in other user defined SQL expressions 
elsewhere in the contract:

```yaml
dataset: postgres_adventureworks/adventureworks/advw/dim_employee

variables:
  START_TS:
    default: date_trunc('day', timestamp '${var.NOW}')
  END_TS:
    default: ${var.START_TS} + interval '1 day'
  USER_DEFINED_FILTER_VARIABLE:
    default: |
      ${var.START_TS} < "created_at"
      AND "created_at" <= ${var.END_TS}

filter: ${var.USER_DEFINED_FILTER_VARIABLE}

checks:
  - failed_rows:
      query: |
        SELECT * 
        FROM "adventureworks"."advw"."dim_employee"
        WHERE SOME_OTHER < CRITERIA
          AND ( ${var.USER_DEFINED_FILTER_VARIABLE} )
```

### Check filters

Applying a filter on an individual check is used to filter the rows on which the check 
is performed.

```yaml
TODO
```

Check filters can be combined with a [dataset filter](#dataset-filter).  The dataset filter 
will be applied on all the checks in the contract, and the check filter will additionally 
filter rows for the individual check. 
