# Link between contract and schema

With the new contracts API, we will revisit the concept of a data source.  Instead of 
combining the connection together with the schema in a data source, contracts will just 
work on a connection.  This will bring the abstractions more in line with what users 
know.

Contract verification operates on a connection.  This implies a selection of a database.
Usually one connection can provide access to multiple schemas.

In the simplest case, a schema is not needed.  Contract verification can run on just the 
table name.  As long as the connection is able to identify the table by its name without 
referring to the schema.

```yaml
dataset: CUSTOMERS
columns:
    - name: id
    ...
``` 

The connection may not have the target schema in the search path and referring to the table 
name may not be sufficient on the connection.  In that case, we should consider to let users 
specify the schema in several ways:

a) In the contract itself:
```yaml
dataset: CUSTOMERS
schema: CSTMR_DATA_PROD
columns:
    - name: id
    ...
```

b) In the API
```python
contract: Contract = Contract.from_yaml_file(file_path=contract_file_path, schema="CSTMR_DATA_PROD")
contract_result: ContractResult = contract.verify(connection=connection, soda_cloud=soda_cloud)
```

c) (still in idea-stage) We can expand this basic API with a file naming convention that uses relative references to 
the schema and connection files.`../schema.yml`
and `../../../connection.yml` leading to for example:

```
+ postgres_localhost_db/
   + connection.sdn.yml
   + soda_cloud.scl.yml
   + schemas/
   |  + CSTMR_DATA_PROD/
   |  |  + schema.yml
   |  |  + datasets/
   |  |  |  + CUSTOMERS.sdc.yml
   |  |  |  + SUPPLIERS.sdc.yml
```
then we can add a simpler API like

```python
import logging
from soda.contracts.contract import Contracts
from soda.contracts.connection import SodaException

try:
    Contracts.verify(["postgres_localhost_db/schemas/CSTMR_DATA_PROD/datasets/*.sdc.yml"])
except SodaException as e:
    logging.exception("Problems verifying contract")
    # TODO ensure you stop your ochestration job or pipeline & the right people are notified
```

This would also fit the CLI tooling.  Using this file name convention, it also makes the connection between the contract
and the database much clearer: The contract is the place where you can extend the databases metadata. 
