#!/usr/bin/env python

from setuptools import setup

package_name = "soda-duckdb"
package_version = "4.0.5b1"
description = "Soda DuckDB V4"

requires = [f"soda-core=={package_version}", "duckdb>=1.2.0", "pytz"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.duckdb": [
            "DuckDBDataSourceImpl = soda_duckdb.common.data_sources.duckdb_data_source:DuckDBDataSourceImpl",
        ],
    },
)
