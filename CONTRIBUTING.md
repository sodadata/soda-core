# Developer cookbook

This document describes all the procedures that developers need to know to 
contribute effectively to the Soda SQL codebase.

## Scripts

In directory `scripts` there are a number of convenience scripts that also 
serve as documentation for developer recipes.  So you might want to check there 
if you're looking for how to complete a particular procedure.  They are typically 
created on a mac and may not be compatible on other developer systems.

## Getting started with a virtual environment

To get started you need a virtual environment.  Use the following script 
to (re)create a `.venv` virtual environment in the root of the project. 

```
scripts/recreate_venv.sh
```

## Running the pre-commit test suite

Before pushing a commit, be sure to run the pre commit test suite.
The test suite is (and should be) kept fast enough (<2 mins) so that 
it doesn't interrupt the developer flow too much. You can use the script 

```
scripts/run_tests.sh
```

The pre commit test suite requires a local PostgreSQL database 
running with certain user and database preconfigured. Use 
`scripts/start_postgres_container.sh` to start a docker container to 
launch a correct PostgreSQL db with the right user and database.

## Pushing a release

Make sure that you you install dev-requirements
```shell
pip-compile dev-requiremnts.in
pip install -r dev-requiremnts.txt
```

Pushing a release is fully automated and only requires to bump the version using `tbump`. For example to release 2.1.0b3, you can use the following command:

```shell
tbump 2.1.0b3
```
