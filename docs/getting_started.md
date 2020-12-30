# Getting started

The simplest way to use soda-sql is to use the CLI. This section explains 
how to install the `soda` command line tool.

The more advance way to use soda-sql is to use the Python programmatic
interface.  TODO add a link

## Prerequisites

### Python 3.7

To check your version of python, run the `python` command
```
$ python
Python 3.7.7 (default, Mar 10 2020, 15:43:33) 
[Clang 11.0.0 (clang-1100.0.33.17)] on darwin
Type "help", "copyright", "credits" or "license" for more information.
```

If you don't, [install it from Python downloads](https://www.python.org/downloads/)

Once Python is installed, you should also have `pip`

## Installing CLI using PyPI
```
$ pip install soda
```

## Create your first project

If at any time during this first project you get stuck, speak up 
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

## Installing CLI from source code

Clone the repository from Github.
```
$ git clone https://github.com/sodadata/soda-sql.git
```
Change to the `soda-sql` directory:
```
$ cd soda
```
Create a new virtual environment, .e.g, ".venv":
```
$ python -m venv .venv
```
Activate the environment:
```
$ source .venv/bin/activate
```
Install the package:
```
$ pip install .
```

