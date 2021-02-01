---
layout: default
title: Installation
parent: Getting Started
nav_order: 1
---

# Installation

The simplest way to use soda-sql is by using the CLI. This section guides you through
the installation steps required to get the `soda` command up and running.

> As an alternative to the CLI, you can use the Python Programmatic Interface, which
> provides you with a more advance way of using Soda SQL.
> See [Programmatic scans]({% link documentation/orchestrate_scans.md %}#programmatic-scans).

## Requirements

The soda-sql CLI requires the following dependencies to be installed on your system:
- Python >=3.7 <3.9
- Pip >=21.0
- postgresql-libs (`libpq-dev` in Debian/Ubuntu, `libpq-devel` in CentOS, `postgresql` on MacOSX)
- _Linux only:_ `libssl-dev` and `libffi-dev` (`libffi-devel` and `openssl-devel` in CentOS)

To check your version of python, run the `python` command
```
$ python --version
Python 3.7.7
$ pip --version
pip 21.0.1 ...
```

If you don't have Python, [install it from Python downloads](https://www.python.org/downloads/) which should also
provide you with `pip`.

To upgrade pip, enter 
```
pip install --upgrade pip
```
There are known issues of soda-sql with pip version 19.  21.0+ works.


## Installing soda CLI globally

```
$ pip install soda-sql
```

## Installing soda CLI in a virtual environment

```
mkdir your_soda_project_dir
cd your_soda_project_dir
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install soda-sql
```

After the installation finishes head over to the [5 Minute Tutorial]({% link getting-started/5_min_tutorial.md %}) to get started.
