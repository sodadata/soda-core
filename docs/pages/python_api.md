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

```
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder(data_source_file_path="../soda/my_postgres_ds.yml")
    .with_contract_yaml_file("../soda/mydb/myschema/mydataset.yml")
    .with_variables({"MY_VAR": "somevalue"})
    .execute()
    .assert_ok()
)
```

### Passing variables

In YAML configuration files like data source and contract files, variables can be specified in the notation `${VAR_NAME}`

It is highly recommended to use variables for credentials in data source YAML files.

Variable names are treated case sensitive.

The environment variables will always be used when variables are resolved.  Optionally user defined variables can be specified in the 
Python API with the `with_variables` method. Eg
```
contract_verification_result: ContractVerificationResult = (
    ContractVerification.builder()
    .with_contract_yaml_file("../soda/mydb/myschema/table.yml")
    .with_variables({"env": "test"})
    .execute()
    .assert_ok()
)
```

User defined variables specified in the API have precedence over environment variables. 
