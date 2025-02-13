# Soda Core Python API

### Installation

We recommend to start in a directory without a `.venv` directory.  

To create a new Python virtual environment use this sequence of command line instructions: 
```
virtualenv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install soda-postgres>4.0a0
```

After installation, the virtual environment can be activated with `source .venv/bin/activate` 
and deactivated with `deactivate`

### Basic API usage

```
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder()
    .with_contract_yaml_file("../soda/mydb/myschema/mydataset.yml")
    .with_variables({"MY_VAR": "somevalue"})
    .execute()
    .assert_ok()
)
```

This API usage assumes a data source is configured via either a relative file path in the contract file 
or using the default relative file path.

This API usage will group all the contract by data source and create a single data source connection to verify 
the contracts for the same data source. Typically, there will be only contracts for a single data source passed. 

### Programmatically specifying the data source file path

If you don't want to use the default file structure to organize your Soda configuration YAML files, you can 
specify a data source programmatically.  Either by referencing the file path or an actual data source in 
the `ContractVerification.builder`

```
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder()
    .with_contract_yaml_file("../soda/mydb/myschema/mydataset.yml")
    .with_data_source_yaml_file("../soda/my_postgres_ds.yml")
    .with_variables({"MY_VAR": "somevalue"})
    .execute()
    .assert_ok()
)
```

### Variables

Use the syntax `${VAR_NAME}` in Soda YAML configuration files like data source and contract files to add dynamic content.
Those variables will be resolved from the variables passed in the API `.with_variables({"VAR_NAME":"value"})` and from 
the environment variables.

It is highly recommended to use variables for credentials in data source YAML files.

User defined variables specified in the API have precedence over environment variables. 

Variable are resolved case sensitive.

Example to provide variable values through the API.
```
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder()
    .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
    .with_variables({"VAR_NAME":"value"})
    .execute()
    .assert_ok()
)
```
