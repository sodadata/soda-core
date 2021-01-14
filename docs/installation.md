# Installation

The simplest way to use soda-sql is by using the CLI. This section guides you through
the installation steps required to get the `soda` command up and running.

> As an alternative to the CLI, you can use the Python Programmatic Interface, which 
> provides you with a more advance way of using Soda SQL. 
> See [Programmatic scans](orchestrate_scans.md#programmatic-scans).

## Requirements

The soda-sql CLI requires the following dependencies to be installed on your system:
- Python >=3.7 <3.9
- postgresql-libs (`libpq-dev` in Debian/Ubuntu, `libpq-devel` in CentOS, `postgresql` on MacOSX)
- _Linux only:_ `libssl-dev` and `libffi-dev` (`libffi-devel` and `openssl-devel` in CentOS)

To check your version of python, run the `python` command
```
$ python --version
Python 3.7.7
```

If you don't have Python, [install it from Python downloads](https://www.python.org/downloads/) which should also
provide you with `pip`.

## Installing soda-sql CLI from PyPI

```
$ pip install soda-sql
```

After the installation finishes head over to the [5 Minute Tutorial](5_min_tutorial.md) to get started.
