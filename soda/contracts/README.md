# Goal and purpose of data contracts in Soda

Soda's goal is to be the best enforcement engine for data contract implementations. For
this purpose we have created a new YAML format that aligns with data contracts from a producer
perspective.  This new format enables a very easy translation from any data contract
YAML language to Soda's enforcable contract language.

# Example

```yaml
dataset: DIM_CUSTOMER

columns:
  - name: id
    data_type: character varying
    unique: true
  - name: cst_size
    data_type: decimal
  - name: cst_size_txt
    valid_values: [1, 2, 3]
  - name: distance
    data_type: integer
  - name: country
    data_type: varchar
    not_null: true
    reference:
      dataset: COUNTRIES
      column: id
  - name: ts

checks:
  - avg(distance) between 400 and 500
```

# Top level keys

| Key | Description | Required | YAML data type |
| --- | ----------- | -------- | -------------- |
| `dataset` | Name of the dataset as in the SQL engine. | Required | string |
| `columns` | Schema specified as a list of columns.  See below for the column format | Required| list of objects |
| `checks` | SodaCL checks.  See [SodaCL docs](https://docs.soda.io/soda-cl/metrics-and-checks.html) | Optional | list of SodaCL checks |

# Column keys

| Key | Description | Required | YAML data type |
| --- | ----------- | -------- | -------------- |
| `name` | Name of the column as in the SQL engine. | Required | string |
| `data_type` | Ensures verification of this physical data type as part of the schema check. Must be the name of the data type as in the SQL engine | Optional | string |
| `not_null` | `not_null: true` Ensures a missing values check | Optional | boolean |
| `missing_*` | Ensures a missing values check and uses all the `missing_*` configurations | Optional | list of strings or numbers |
| `valid_*` * `invalid_*` | Ensures a validity check and uses all the `valid_*` and `invalid_*` configurations | Optional | list of strings or numbers |
| `unique` | `unique: true` ensures a uniqueness check | Optional | boolean |
| `reference` | Ensures a reference check that ensures values in the column exist in the referenced column.See section reference keys below. | Optional | object |

# Reference keys

| Key | Description | Required | YAML data type |
| --- | ----------- | -------- | -------------- |
| `dataset` | Name of the reference dataset as in the SQL engine. | Required | string |
| `column` | Name of the column in the reference dataset. | Required | string |
| `samples_limit` | Limit of the failed rows samples taken. | Optional | number |

# Contract to SodaCL translation

## The schema check

A contract verification will always check the schema.  The contract schema check will verify that the list of columns
in the database dataset matches with the columns in the contract file.  All columns listed in the contract
are required and no other columns are allowed.  

Optionally, if the `data_type` property is specified in the column, the data type will be checked as well as part of
the schema check.

The ordering and index of columns is ignored.

## Other column checks

`not_null`, `missing_values`, `missing_format` or `missing_regex` will ensure a single check
`missing_count({COLUMN_NAME}) = 0` on that column with all the `missing_*` keys as configuration.

Presence of `valid_*`, `invalid_*` column keys will ensure a single validity check `invalid_count({COLUMN_NAME}) = 0`
on that column with all `valid_*`, `invalid_*` keys as configuration.

`unique: true` will ensure a SodaCL check `duplicate_count({COLUMN_NAME}) = 0`

## SodaCL checks section

The YAML list structure under `checks:` will just be copied to the SodaCL checks file as-is.

# Contract enforcement

"Enforcement" of a contract comes down to verifying that a certain dataset (like eg a table) complies with the specification in
the contract file.  When the contract does not comply, the data owner and potentially the consumers should be notified.

> Known limitation: At the moment there possibility to verify contracts using the CLI. Only a 
> Python programmatic API is available. 

In your python (virtual) environment, ensure that the libraries `soda-core` and `soda-core-contracts` are available
as well as the `soda-core-xxxx` library for the SQL engine of your choice.

To verify if a dataset complies with the contract, here's the code snippet.

```python
from contracts.data_contract_translator import DataContractTranslator
from soda.scan import Scan
import logging

# Read your data contract file as a Python str
with open("dim_customer_data_contract.yml") as f:
    data_contract_yaml_str: str = f.read()

# Translate the data contract into SodaCL
data_contract_parser = DataContractTranslator()
sodacl_yaml_str = data_contract_parser.translate_data_contract_yaml_str(data_contract_yaml_str)

# Logging or saving the SodaCL YAMl file will help with debugging potential scan execution issues 
logging.debug(sodacl_yaml_str)

# Execute the contract SodaCL in a scan
scan = Scan()
scan.set_data_source_name("SALESDB")
scan.add_configuration_yaml_file(file_path="~/.soda/my_local_soda_environment.yml")
scan.add_sodacl_yaml_str(sodacl_yaml_str)
scan.execute()
scan.assert_all_checks_pass()
```
