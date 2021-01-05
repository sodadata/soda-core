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

2\) Run `soda create` and pass a project name and one of the supported warehouse types

```
$ soda create ~/soda_projects/my_first_soda_project postgres
Soda CLI version 2.0.0 beta
Creating project dir /Users/tom/soda_projects/my_first_soda_project ...
Creating project file /Users/tom/soda_projects/my_first_soda_project/soda_project.yml ...
Creating profile dir /Users/tom/soda_projects/my_first_soda_project/my_first_soda_project ...
Adding profile my_first_soda_project to existing /Users/tom/.soda/profiles.yml
Please review and update the 'my_first_soda_project' profile in ~/.soda/profiles.yml
Then run 'soda create'
```

Next, review and update the `my_first_soda_project` profile in `~/.soda/profiles.yml`

See
 * [soda init command](cli.md#init) to learn more about `soda init`
 * [Warehouses](warehouses.md) to learn more about ~/.soda/profiles.yml
 * [Project](project.md) to learn more about soda_project.yml

3\) Test your connection with 

(TODO : under construction. You can continue the tutorial without this verification step)
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
