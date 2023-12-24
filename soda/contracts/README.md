# Soda Data Contracts

A data contract is a YAML file that describes the data in a dataset. Soda is used to 
verify that new data in a dataset is matching the schema and other data quality properties 
in the contract.

A dataset is a table or other tabular datastructure accessible via a SQL connection.

# Soda data contract

Example:

```yaml
dataset: DIM_CUSTOMER

columns:
    
  - name: id
    data_type: character varying
    checks:
      - type: missing
      - type: unique
    
  - name: cst_size_txt
    checks:
      - type: invalid
        valid_values: [1, 2, 3]
    
  - name: distance
    data_type: integer
    checks: 
      - type: avg
        fail_when_not_between: [50, 150]
          
  - name: country
    data_type: varchar
    checks:
      - type: no_missing_values
      - type: invalid
        valid_values_column: 
            dataset: COUNTRIES
            column: id

checks:
  - type: row_count
    fail_when_not_between: [100, 500]
  - type: freshness_in_hours
    fail_when_greater_than: 6
```

[Learn more about the Soda data contract checks](docs/contract_checks.md)

# Data contract API

```python
from soda.contracts.connection import Connection
from soda.contracts.contract import Contract
from soda.contracts.contract_verification_result import ContractVerificationResult
from soda.contracts.soda_cloud import SodaCloud

try:
    soda_cloud: SodaCloud = SodaCloud.from_yaml_file("./soda_cloud.scl.yml")
    with Connection.from_yaml_file("./postgres_localhost_dev.scn.yml") as connection:
        contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
        contract_verification_result: ContractVerificationResult = contract.verify(connection, soda_cloud)
        contract_verification_result.assert_no_errors()
        if contract_verification_result.has_failures():
            # make the orchestration job fail and report contract check failures.
except SodaException as e:
    # make the orchestration job fail and report contract verification errors.
```

[Learn more about the Soda contract API](docs/contract_api.md)
