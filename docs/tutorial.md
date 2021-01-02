# Tutorial

If at any time during this tutorial you get stuck, speak up 
in the [getting-started Slack channel](slack://channel?id=C01HYL8V64C&team=T01HBMYM59V) or 
[post an issue on GitHub](https://github.com/sodadata/soda-sql/issues/new)

1\) Open a command line and enter `soda` to check your soda command line tool.

```
$ soda
Usage: soda [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  init
  create
  verify
  scan
```

If you don't get this output, check out [getting started](getting_started.md) 
for installation instructions

2\) Run soda init and pass a project name and one of the supported warehouse types

```
$ soda init my_first_project -profile first -warehousetype snowflake
Creating ~/.soda/profiles.yml
Adding snowflake profile first in ~/.soda/profiles.yml
Creating ./my_first_project/soda_project.yml
Please review and update the 'first' snowflake profile in ~/.soda/profiles.yml
Then run 'soda create'
```

[See 'Warehouses' to learn more about ~/.soda/profiles.yml](warehouses.md)

[See 'Project' to learn more about soda_project.yml](project.md)

3\) Test your connection with 

```
$ soda verify my_first_project
Connection to snowflake ok
```

4\) Use the `create` command to create a `scan.yml` for each table in your warehouse

```
$ soda create my_first_project
Scanning CUSTOMERS...
Creating my_first_project/customers/scan.yaml
Scanning SUPPLIERS...
Creating my_first_project/suppliers/scan.yaml
Scanning SALE_EVENTS...
Creating my_first_project/sale_events/scan.yaml
Done
```

5\) Review and update [the scan.yaml files](scan.md)

6\) Now you can run a scan on any of the tables created like this:

```
$ soda scan my_first_project suppliers
TODO show some example output
```
