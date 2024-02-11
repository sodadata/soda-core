# Contract basics

The basics for authoring Soda contract YAML files.

### Simple contract file explained

A Soda data contract contains:

* Identification of the physical table or view (`dataset`)
* The schema as a list of columns (`columns`)
* Data quality checks on column and dataset level
* Other metadata not related to contract verification

Soda contracts is a Python library that verifies if the schema and data in a dataset/table/view
passes checks in the contract.

```yaml
dataset: CUSTOMERS

columns:
- name: id
- name: size
  checks:
  - type: invalid_count
    valid_values: ['S', 'M', 'L']

checks:
- type: row_count
```

### Soda contract file naming convension

We recommend that to use the extension `.sdc.yml` for all Soda data contract files.
This will make it easier for tools to associate the contract files with the
appropriate schema file to get better editing experience in your IDE.

### Setting up code completion in VSCode

TODO https://dev.to/brpaz/how-to-create-your-own-auto-completion-for-json-and-yaml-files-on-vs-code-with-the-help-of-json-schema-k1i

### Setting up code completion in PyCharm

TODO: https://www.jetbrains.com/help/pycharm/json.html#ws_json_schema_add_custom
