# Exceptions vs error logs

In general the principle is that contract verification aims to be resilient,
record any logs and continue to report as many problems in a single execution.

This is realized by suppressing exceptions and collecting all the logs until the
end of the `contract.verify` method. There any error logs or check failures will
cause an exception to be raised. The SodaException raised at the end of the
`contract.verify` method will list all the errors and check failures in a
single SodaException.

So for any of the following problems, you will get an exception being
raised at the end of the contract.verfiy method:
* Connection
  * Connection YAML or configuration issues (includes variable resolving problems)
  * Connection usage issues (can't reach db or no proper permissions)
* SodaCloud issues (only if used)
  * SodaCloud YAML or configuration issues (includes variable resolving problems)
  * SodaCloud usage issues (can't reach Soda online or no proper credentials)
* Contract
  * Contract YAML or configuration issues (includes variable resolving problems)
  * Contract verification issues
  * Check failures

In the next recommended API usage, please note that exceptions suppressed in
Connection, SodaCloud and contract parsing are passed as logs (Connection.logs,
SodaCloud.logs, Contract.logs) in to the `contract.verify` method.

```python
connection_file_path = 'postgres_localhost.scn.yml'
contract_file_path = 'customers.sdc.yml'
try:
    soda_cloud: SodaCloud = SodaCloud.from_environment_variables()
    with Connection.from_yaml_file(file_path=connection_file_path) as connection:
        contract: Contract = Contract.from_yaml_file(file_path=contract_file_path)
        contract_result: ContractResult = contract.execute(connection=connection, soda_cloud=soda_cloud)
        # contract verification passed
except SodaException as e:
    # contract verification failed
    logging.exception(f"Contract verification failed: {e}", exc_info=e)
```
