# Exceptions vs error logs

For any type of problem, an exception will be raised.  This includes: 
* YAML syntax problems parsing a connection, soda cloud or contract YAML file
* Problems resolving variables in YAML files for connections, soda cloud and contracts
* Problems interpreting a connection, soda cloud or contract YAML 
* Query problems
* Check evaluation exceptions
* Check failures

There are unrecoverable exceptions and there are recoverable exceptions.
As long as exceptions are unrecoverable, an exception is thrown immediately.
Once passed the unrecoverable errors, the API does an attempt to collect as 
many contract YAMl interpretation problems and check failures in one execution.

In development of the contract, it makes sense to provide 
the engineer with as many errors in one contract verification so that all 
problems can be fixed before retrying. 

In production, it's often hard or impossible to re-run the contract verification.
So in that case it is crucial that we provide as much diagnostic information as 
possible from a single contract verification. With that use case in mind we 
have to be as resilient as possible for errors to provide as much feedback in 
one execution.

```python
connection_file_path = 'postgres_localhost.scn.yml'
contract_file_path = 'customers.sdc.yml'
try:
    soda_cloud: SodaCloud = SodaCloud.from_environment_variables()
    with Connection.from_yaml_file(file_path=connection_file_path) as connection:
        contract: Contract = Contract.from_yaml_file(file_path=contract_file_path)
        contract_result: ContractResult = contract.verify(connection=connection, soda_cloud=soda_cloud)
        # contract verification passed
except SodaException as e:
    # contract verification failed
    logging.exception(f"Contract verification failed: {e}", exc_info=e)
```

Creation of the SodaCloud object should not raise exceptions. Also not 
when variables cannot be resolved.  Instead, these type of problems (even 
unrecoverable ones) should be collected as logs in the SodaCloud object.
If a SodaCloud object has errors, it should pass them as logs to the 
contract result.

Same for contract parsing. Any contract parsing errors should be passed 
from the contract to the contract result in the verify method.

At the end of the `contract.verify` method, there is an assertion that there 
are no execution errors.  Then all the errors will be listed in a single 
SodaException.
