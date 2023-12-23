# Connection and contract API

The main API was designed to accommodate the minimal, straightforward way to execute the 
essential functionality.  That way all other functionalities can be layered on top.

The library functionality is all based on a DBAPI connection. We expose that to the user 
as a plain resource using the with statement

A wrapper around the DBAPI connection is needed to handle the SQL differences. It's 
anticipated that initially the implementation will be based on the existing Soda Core 
DataSource and Scan.  But that later there will be direct connection implementations 
for each database.

The contract verification API is designed to run on a connection.

```python
soda_cloud: SodaCloud = SodaCloud.from_environment_variables()
with Connection.from_yaml_file('postgres_localhost.scn.yml') as connection:
    contract: Contract = Contract.from_yaml_file('customers.sdc.yml')
    contract_verification_result = contract.verify(connection)
    soda_cloud.send(contract_verification_result)
```
