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

### Variables in a contract verification

Contracts can refer to variables`${var.VAR_NAME}`.  To pass variables in the Soda Python API for 
contract verification, use parameter `variables={"MY_VAR": "somevalue"}`

See ['Contract variables' in the writing check files page](writing_check_files.md#contract-variables)
