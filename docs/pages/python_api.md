# Soda Core Python API

### Install Soda Core in a virtual environment

See [Install Soda Core in a Python virtual environment](./install.md)

### Basic API usage

```
contract_verification_session_result: ContractVerificationSessionResult = (
    ContractVerificationSession.execute(
        contract_yaml_sources=[YamlSource.from_file_path("soda/mydb/myschema/mytable.c.yml")],
        data_source_yaml_sources=[YamlSource.from_file_path("soda/mydb.ds.yml")],
        variables={"MY_VAR": "somevalue"},
    )
```

You have to ensure that the data sources that are referred to in the contracts are available either to the 
contract verification session. Contracts refer to data sources by name. 

This API usage will group all the contract by data source and create a single data source connection to verify 
the contracts for the same data source. Typically, there will be only contracts for a single data source passed. 

### Contract verification session execute full signature

```
class ContractVerificationSession:

    @classmethod
    def execute(
        cls,
        contract_yaml_sources: list[YamlSource],
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_source_impls: Optional[list["DataSourceImpl"]] = None,
        data_source_yaml_sources: Optional[list[YamlSource]] = None,
        soda_cloud_impl: Optional["SodaCloud"] = None,
        soda_cloud_yaml_source: Optional[YamlSource] = None,
        soda_cloud_skip_publish: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
    ) -> ContractVerificationSessionResult:
```



### Variables

Use the syntax `${VAR_NAME}` in Soda YAML configuration files like data source and contract files to add dynamic content.
Those variables will be resolved from the variables passed in the API `variables={"MY_VAR": "somevalue"}` and from 
the environment variables.

It is highly recommended to use variables for credentials in data source YAML files.

User defined variables specified in the API have precedence over environment variables. 

Variable are resolved case sensitive.
