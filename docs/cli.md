# Soda's Command Line Interface (CLI)

## Initialize a warehouse configuration directory 

To make it easier to get started with soda-sql, 
`soda init` will initialize analyse tables in a warehouse and 
create directories with scan configuration files for each table.

`soda init [OPTIONS] PROFILE DIRECTORY`

`PROFILE` indicates which profile and hence which database to use. See 
[Warehouses](warehouses.md) to learn more about configuring warehouse 
profiles.yml

`DIRECTORY` is a directory in which the scan configuration files will 
be generated.  For each table, a directory and scan.yml file will be 
created in it if it doesn't exist. If a scan.yml file already exists for 
a particular table, it will not be changed.

Option `target=TARGET` Use target to specify the warehouse configuration 
properties to use.
  
## Execute a scan 

TODO doc how to specify the CLI parameters for the scan

`soda scan ...`

For generic scan parameters descriptions and other details see [Scan](scan.md) 
