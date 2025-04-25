# Variables

### When to use variables?

Variables are used to:

* Verify contract checks on a time partition
* Declare a value once and use it multiple times
* Verify the same contract on different datasets
* Make other contract values dynamic

### Declaring and referring to variables
Use the syntax `${var.VARNAME}` to refer to variables in a contract.

Declare variables in a contract in the `variables` section. Keys are the variables.  
A `default` value is the one optional configuration for variables

Example
```yaml
dataset: postgres_adventureworks/adventureworks/${var.DATASET_SCHEMA}/${var.DATASET_PREFIX}_employee

variables: 
  DATASET_SCHEMA:
    default: advw
  DATASET_PREFIX:

columns:
  - name: id
```

All variables used in a contract have to be declared.

### Providing variable values

All variables declared are required to get a value.

Variable values are provided in one of 3 ways: 
* Provided as parameter when starting a contract verification in the CLI or Python API
  * This may be indirectly via the Soda Cloud test contract form
  * This may be indirectly via the Soda Cloud schedule
* Using the `default` key in the variable declaration in the contract YAML

The `default` key can also refers to other variables, as long as there are no circular 
references.

Variable are resolved case sensitive.

Provide variables in the CLI
```shell
> soda contract verify --set THE_VAR=thevalue
```

Provide variables in the Python API
```python
contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
    ...,
    variables={"THE_VAR": "thevalue"},
    ...
)
```

### The ${soda.NOW} variable

For building time partition filters, see [Building a time partition filter](filters.md#building-a-time-partition-filter) 

There is a separate `soda` namespace that provides constant values that can not be given a 
user defined value.

`${soda.NOW}` has the current timestamp in ISO8601 format as default.  

If you want the current time by default value and optionally allows users to supply 
a timestamp value, use:
```yaml
dataset: postgres_adventureworks/adventureworks/${var.DATASET_SCHEMA}/${var.DATASET_PREFIX}_employee

variables: 
  NOW:
    default: ${soda.NOW}
```

### Environment variables are not resolved in a contract

Variables are not to be used for data source credentials.  Environment variables or a secret store should 
be used for that instead.  References to environment variables like eg `${env.ERROR}` will be ignored in 
contract files.
