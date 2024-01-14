# Connection and contract API

The contract API was designed to accommodate the provide a way to execute the verification of a 
contract in a minimal way so that it can be used and combined in as many scenarios and use cases 
as possible.

Guiding principles for this API are:
* Easy to read and understand
* Simple way to stop the pipeline in case of problems (problems are both contract verification 
  execution exceptions as well as check failures)
* Simple way to introspect the contract verification results
* Problems with SodaCloud or database connections should fail fast as these are not recoverable
* For contract verification, as many problems as possible should be collected and reported in one go.
* Simple way to leverage the existing Soda Core engine and optionally provide new implementations for
  contract verification later on.

From a concepts point of view, we switch from using the notion of a data source to using a connection.
If the schema has to be used, it has to be referenced separately: either in the contract file, as a 
contract verification parameter or some other way. 

A wrapper around the DBAPI connection is needed to handle the SQL differences. 
It's anticipated that initially the implementation will be based on the existing Soda Core 
DataSource and Scan.  But that later there will be direct connection implementations 
for each database.

The returned connection is immediately open.

```python
import logging
from soda.contracts.connection import Connection, SodaException
from soda.contracts.contract import Contract, ContractResult
from soda.contracts.soda_cloud import SodaCloud

connection_file_path = 'postgres_localhost.scn.yml'
contract_file_path = 'customers.sdc.yml'
try:
    soda_cloud: SodaCloud = SodaCloud.from_environment_variables()
    with Connection.from_yaml_file(file_path=connection_file_path) as connection:
        contract: Contract = Contract.from_yaml_file(file_path=contract_file_path)
        contract_result: ContractResult = contract.verify(connection=connection, soda_cloud=soda_cloud)
except SodaException as e:
    logging.exception("Problems verifying contract")
    # TODO ensure you stop your ochestration job or pipeline & the right people are notified
```
