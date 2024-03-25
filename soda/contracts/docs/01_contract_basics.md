# Contract basics

The basics for authoring Soda contract YAML files.

> Prerequisites: To write contract YAML files, you will need:
> * A YAML text editor
> * Optional: a local development environment to test contract YAML files
> * Optional: a git repository to version control your contract YAML file

Contents:
  * [Simple contract file explained](#simple-contract-file-explained)
  * [Soda contract file naming convension](#soda-contract-file-naming-convension)
  * [Setting up code completion in PyCharm](#setting-up-code-completion-in-pycharm)
  * [Setting up code completion in VSCode](#setting-up-code-completion-in-vscode)


### Simple contract file explained

A Soda data contract contains:

* Identification of the physical table or view (`dataset`)
* The schema as a list of columns (`columns`)
* Data quality checks on column and dataset level
* Other metadata not related to contract verification

Soda contracts is a Python library that verifies if the schema and data in a dataset/table/view
passes checks in the contract.

```yaml
dataset: DIM_CUSTOMER

columns:

- name: id
  data_type: character varying
  checks:
  - type: no_missing_values
  - type: no_duplicate_values
  - type: no_invalid_values
    valid_regex_sql: '^[A-Z0-9]{8}$'

- name: distance
  data_type: integer
  checks:
  - type: avg
    must_be_between: [50, 150]

- name: country_id
  checks:
  - type: invalid_percent
    valid_values_column:
      dataset: COUNTRIES
      column: id
    must_be_less_than: 5

- name: country_id
  checks:
  - type: freshness_in_hours
    must_be_less_than: 6

checks:
- type: rows_exist
```

### Setting up code completion in VSCode

Ensure you have the YAML extension installed.

Download [the schema file](./soda/contracts/soda_data_contract_schema_1_0_0.json), put it somewhere relative to the contract YAML file and
add `# yaml-language-server: $schema=./soda_data_contract_schema_1_0_0.json` on top of the contract YAML file like this:

```yaml
# yaml-language-server: $schema=./contract_schema.json

dataset: CUSTOMERS

columns:
    - ...
```

Alternatively see https://dev.to/brpaz/how-to-create-your-own-auto-completion-for-json-and-yaml-files-on-vs-code-with-the-help-of-json-schema-k1i

### Setting up code completion in PyCharm

Download [the schema file](./soda/contracts/soda_data_contract_schema_1_0_0.json), put it somewhere in your project or on yourr file system.

Go to: [Preferences | Languages & Frameworks | Schemas and DTDs | JSON Schema Mappings](jetbrains://Python/settings?name=Languages+%26+Frameworks--Schemas+and+DTDs--JSON+Schema+Mappings)

And add a mapping between *.sdc.yml files and the schema

Alternatively see https://www.jetbrains.com/help/pycharm/json.html#ws_json_schema_add_custom
