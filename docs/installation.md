# Installation

The simplest way to use soda-sql is to use the CLI. This section explains
how to install the `soda` command line tool.

The more advance way to use soda-sql is to use the Python programmatic
interface.  TODO add a link

## Requirements

The soda CLI requires the following packages to be installed on your system:
- Python 3.7+
- postgresql-libs (`libpq-dev` in Debian/Ubuntu, `libpq-devel` in CentOS, `postgresql` on MacOSX)
- _Linux only:_ `libssl-dev` and `libffi-dev` (`libffi-devel` and `openssl-devel` in CentOS)

To check your version of python, run the `python` command
```
$ python --version
Python 3.7.7
```

If you don't have Python, [install it from Python downloads](https://www.python.org/downloads/)

Once Python is installed, you should also have `pip`.

## Installing CLI using PyPI

```
$ pip install soda-sql
```

If this works for you, you can continue with the [Tutorial](5_min_tutorial.md)
