When you run a scan, Soda SQL uses the configurations in your scan YAML file to prepare, then run SQL queries against data in your warehouse. The default tests and metrics Soda SQL configured when it created the YAML file focus on finding missing, invalid, or unexpected data in your tables.

Each scan requires the following as input:
- a Warehouse YAML file, which represents a connection to your SQL engine
- a scan YAML file, including its path, which contains the metric and test instructions that Soda SQL uses to scan tables in your warehouse

#### Example command 
```shell
$ soda scan warehouse.yml tables/demodata.yml
```