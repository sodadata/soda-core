#!/usr/bin/env python

from setuptools import setup

package_name = "soda-sqlserver"
package_version = "4.0.0b9"
description = "Soda SQL Server V4"

requires = [
    f"soda-core=={package_version}",
    "pyodbc",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.sqlserver": [
            "SqlServerDataSourceImpl = soda_sqlserver.common.data_sources.sqlserver_data_source:SqlServerDataSourceImpl",
        ],
    },
)
