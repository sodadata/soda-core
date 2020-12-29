# Getting started

The simplest way to use soda-sql is to use the CLI. This section explains 
how to install the `soda` command line tool.

The more advance way to use soda-sql is to use the Python programmatic
interface.  TODO add a link 

## Installing CLI using PyPI

```
$ pip install soda-sql
```

## Installing CLI from source code

- Clone the repository from Github.

```
$ git clone https://github.com/sodadata/soda-sql.git
```

- Change to the `soda-sql` directory:

```
$ cd soda-sql
```

- Create a new virtual environment for `soda-sql`, .e.g, "soda_env":

```
$ python -m venv soda_env
```

- Activate the environment:
```
$ source soda_env/bin/activate
```

- Install the package:
```
$ pip install .
```

- Run the CLI application:

```
$ soda-sql
Usage: soda-sql [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  check
  initialize
  scan
```
