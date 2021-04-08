#!/usr/bin/env bash
rm -fr build
rm -fr dist
rm -fr soda_sql_core.egg-info
pip uninstall -y soda-sql-core
python setup.py sdist
pip install .
