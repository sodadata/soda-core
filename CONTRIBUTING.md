# Developer documentation

This is a guide providing pointers to all tasks related to the development of Soda Checks.

## Proposed project folder structure

soda-core : default name of the root project folder as the git repo will be named soda-core.  But contributors must be able to use a different top level folder name
 + `soda-core` : root src folder for the soda-core package
 + `soda-core-tests` : tests of the soda-core package
 + `packages`
   + `soda-core-postgres`
     + `soda-core-postgres` : root src folder for the soda-core-postgres package
     + `tests` : tests of the soda-core-postgres package
   + all other warehouses similar to postgres
   + `soda-core-ml`
     + `soda-core-ml` : root src folder for the soda-core-ml package
     + `tests` : tests of the soda-core-ml package

## Cloning the repo

```
> git clone git@github.com:sodadata/soda-sql-v3.git
Cloning into 'soda-sql-v3'...
remote: Enumerating objects: 5319, done.
remote: Counting objects: 100% (5319/5319), done.
remote: Compressing objects: 100% (2841/2841), done.
remote: Total 5319 (delta 3358), reused 4187 (delta 2253), pack-reused 0
Receiving objects: 100% (5319/5319), 1.44 MiB | 3.69 MiB/s, done.
Resolving deltas: 100% (3358/3358), done.
>
```

## Requirements

You'll need Python 3.8+
```
> python --version
Python 3.8.12
>
```

In case you need to go and get it, see [Python downloads](https://www.python.org/downloads/)

Although not required, we recommend to use [pyenv](https://github.com/pyenv/pyenv) to more easily manage multiple Python
versions.

## Creating the virtual environment

There is a convenience script `scripts/recreate_venv.sh` to create the virtual environment:

```
> scripts/recreate_venv.sh
Requirement already satisfied: pip in ./.venv/lib/python3.8/site-packages (21.1.1)
Collecting pip
  Using cached pip-21.3.1-py3-none-any.whl (1.7 MB)
Installing collected packages: pip
  Attempting uninstall: pip

...loads of output and downloading going on here...

Successfully installed Jinja2-2.11.3 MarkupSafe-2.0.1 cffi-1.15.0 click-8.0.3 cryptography-3.3.2 pycparser-2.21 ruamel.yaml-0.17.17 ruamel.yaml.clib-0.2.6 soda-sql-core-v3-3.0.0-prerelease-1
>
```

The `scripts/recreate_venv.sh` script will install the dependencies in your virtual environment.  See the contents
as inspiration if you want to manage the virtual environment yourself.

## Activating the virtual environment

```
> source .venv/bin/activate
(.venv) >
```

## Running the test suite

Running the test suite requires a Postgres DB running on localhost having a user `sodasql`
without a password, database `sodasql` with a `public` schema.  Simplest way to get one
up and running is `scripts/start_postgres_container.sh` which launches the container
`packages/postgres/docker-compose.yml`.

Before pushing commits or asking to review a pull request, we ask that you verify successful execution of
Before pushing commits or asking to review a pull request, we ask that you verify successful execution of
the following test suite on your machine.

```
> python -m pytest
=================================================================================================================== test session starts ====================================================================================================================
platform darwin -- Python 3.8.12, pytest-6.0.2, py-1.10.0, pluggy-0.13.1 -- /Users/tom/Downloads/soda-sql-v3/.venv/bin/python
cachedir: .pytest_cache
metadata: {'Python': '3.8.12', 'Platform': 'macOS-11.6-x86_64-i386-64bit', 'Packages': {'pytest': '6.0.2', 'py': '1.10.0', 'pluggy': '0.13.1'}, 'Plugins': {'Faker': '8.1.2', 'html': '3.1.1', 'metadata': '1.11.0', 'cov': '2.10.1'}}
rootdir: /Users/tom/Downloads/soda-sql-v3/core, configfile: tox.ini
plugins: Faker-8.1.2, html-3.1.1, metadata-1.11.0, cov-2.10.1
collected 60 items

core/tests/unit/test_checks_yaml_parser.py::test_checks_parsing | "/Users/tom/Code/soda-sql-v3/core/tests/unit/test_checks_yaml_parser.py:9" test_checks_parsing
PASSED                                                                                                                                                                               [  1%]

...loads of output...

============================================================================================================= 60 passed, 142 warnings in 2.70s =============================================================================================================
(.venv) >
```

### Testing cross cutting concerns

There are a couple of cross cutting concerns that need to be tested over a variety of functional
test scenarios.  To do this, we introduce environment variables that if set, activate the cross
cutting feature while executing the full test suite.

* `export WESTMALLE=LEKKER` : activates soda cloud connection
* `export CHIMAY=YUMMIE` : activates local storage of files
* `export ROCHEFORT=HMMM` : activates notifications

## Testing with Tox

TODO : For the full test matrix of test suite executions, we ll be using [Tox](https://tox.wiki/en/latest/)
