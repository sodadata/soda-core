# Soda's Command Line Interface (CLI)

## Create

The `soda create` CLI command helps you to get started with a new warehouse 
setup. It will create an initial warehouse directory, the warehouse configuration 
file and the environment variables in `~/.soda/env_vars.yml`.

`soda create` will never delete files or remove file contents.  It only adds things 
that are not yet present and the command reports on what it does. 

`soda create --help` to learn more

Examples 
  
## Init 

The `soda init` CLI command is the next step to help you get started.  It finds 
tables in your warehouse, inspects them and creates appropriate `scan.yml` files 
for each table.

`soda init` 

`soda init --help` to learn more

See section [Scan](scan.md) to learn more on scan file configurations. 
