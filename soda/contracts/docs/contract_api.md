# Contract API

## Creating a connection

A contract is verified on a connection. A connection can be created based 
on a YAML file, a YAML string or a dict. Use one of the `Connection.from_*` methods
to create a connection.

```python
with Connection.from_yaml_file("./postgres.scn.yml") as connection:
    # Do stuff with connection
```

### Connection YAML configuration files

It's good practice to use environment variables in your YAML files to protect your credentials.  
Use syntax `${POSTGRES_PASSWORD}` for resolving environment variables.  Variable resolving is 
case sensitive. We recommend using all upper case and underscores for naming environment variables. 

For connection configuration YAML files, we recommend to use extension `scn.yml` (Soda ConnectioN) because 
the editor tooling support will likely will be coupled via that extension. 

### A postgres connection

To create a postgres connection with a username/password:
```yaml
type: postgres
host: localhost
username: johndoe
password: ***
```

> TODO: show examples of all other types of postgres connection configurations.

## Verifying a contract

To verify a contract, you need a [connection](#creating-a-connection) and a contract file:

```python
contract: Contract = Contract.from_yaml_file("./customers.sdc.yml")
```

### Contract YAML files

You can use variables to get contracts variations. Use syntax `${SCHEMA}` for resolving environment variables in a contract YAML file.  
Variable resolving is case sensitive. We recommend using all upper case and underscores for naming environment variables. 

For contract YAML files, we recommend to use extension `sdc.yml` (Soda Data Contract) because 
the editor tooling support will likely will be coupled via that extension.

## API to create a SodaCloud



### From YAML file or YAML string

### From env vars only

## Environment variables and defaults

Precedence of values 
* lower case env vars
* upper case env vars
* provided configuration value
* default value

## Soda Cloud configuration properties

What is it?  Where to get it?  What's the default value?

host (default cloud.soda.io)
api_key_id (required)
api_key_secret (required)
scheme (default https)
