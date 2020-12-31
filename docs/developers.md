# Developers

## Scripts

`scripts` directory contains convenience scripts for developers.

If you're looking for something that can be considered part of the developer workflow,
check out the `scripts` directory. 

## Virtual environment

This project uses a Python virtual environment.

(Re)create the soda-sql virtual environment with `scripts/recreate_venv.sh`

The virtual environment will be created in directory `{project_root}/.venv`

To activate the virtual environment, run `. .venv/bin/activate` in the project root. 

## Running Tests

The `tests` package contains the different test suites

`python -m pytest tests/local` is the default test suite that must pass before every commit.  It 
runs all the independent tests as well as the tests that are dependent on a local PostgreSQL 
DB.  `tests/postgres_container/docker-compose.yml` can be used to start a local PostgreSQL DB.  You 
can use `scripts/start_test_postgres_db.sh` as a convenience script to launch that container.

`python -m pytest tests/local/independent` runs only the tests that are not dependent 
on anything else except the python interpreter

`python -m pytest tests/warehouses` runs all sql tests on all the warehouses.  Note that to 
run this test suite you have to configure the credentials in `~/.soda/profiles.yml`.  If 
the file does not exist, an initial default version will be created.

## Running tests in your IDE

If you want to run sql tests on a different warehouse than postgres, use 
environment variable `SODA_TEST_TARGET` or use `tests/.env`  
See `tests/.env_example` for more details

## Running Tests with Tox

Tox will start a Postgres database as a docker image and stop it after the tests are finished.

To run all unit tests simply execute the following command:

```
$ tox
```

To run tests only a specific test suite:

```
$ tox -e cli
```

Runs only tests from the CLI suite.

To run a specific test method:

```
$ tox -e postgres -- -k test_distinct
```

Runs test method "test_distinct" from postgres" test suite only.

You may also add extra parameters for logging and debugging. 

This shows everything written to the standard output (even if tests don't fail):

```
$ tox -- -s
```

This changes the log level:

```
$ tox -- --log-cli-level=DEBUG
```

HTML reports will be available in the directory `./reports/`. It include tests and coverage.

## Deploying to PyPI:

- Create an account on [PyPI](https://pypi.org/) and [TestPyPI](https://test.pypi.org/) (these are separated accounts) if you don't have them.

- Build the distribution:

```
$ python setup.py sdist bdist_wheel
```

On success, you should have generated the distribution files under the folder `./dist`, e.g.:

```
$ ls ./dist
soda_sql-0.1.0-py3-none-any.whl  soda-sql-0.1.0.tar.gz
```

- Upload the distribution to TestPyPI server:

```
$ twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

If it uploads successfully with no issues, that means you can publish on the live server.

- Upload the distribution to PyPI server:

```
$ twine upload dist/*

```

- Now you can install the distribution from PyPI:

```
$ pip install soda
```
