# Contract basics

The basics for authoring Soda contract YAML files.

> Prerequisites: To write contract YAML files, you will need:
> * A YAML text editor
> * Optional: a local development environment to test contract YAML files
> * Optional: a git repository to version control your contract YAML file

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
  - type: missing
  - type: duplicate_count

- name: cst_size_txt
  checks:
  - type: invalid_count
    valid_values: [1, 2, 3]

- name: distance
  data_type: integer
  checks:
  - type: avg
    fail_when_not_between: [50, 150]

- name: country
  data_type: varchar
  checks:
  - type: missing_count
  - type: invalid_count
    valid_values_column:
      dataset: COUNTRIES
      column: id

checks:
- type: row_count
  fail_when_not_between: [100, 500]
- type: freshness_in_hours
  fail_when_greater_than: 6
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

## Setting up code completion in PyCharm

Download [the schema file](./soda/contracts/soda_data_contract_schema_1_0_0.json), put it somewhere in your project or on yourr file system.

Go to: [Preferences | Languages & Frameworks | Schemas and DTDs | JSON Schema Mappings](jetbrains://Python/settings?name=Languages+%26+Frameworks--Schemas+and+DTDs--JSON+Schema+Mappings)

And add a mapping between *.sdc.yml files and the schema

Alternatively see https://www.jetbrains.com/help/pycharm/json.html#ws_json_schema_add_custom
