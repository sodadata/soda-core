#!/usr/bin/env python

from setuptools import setup

package_name = "soda-databricks"
package_version = "4.0.4b18"
description = "Soda Databricks V4"

requires = [
    f"soda-core=={package_version}",
    "databricks-sql-connector<4.2.2",  # Temporary fix to avoid issues with new version (4.2.2)
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.databricks": [
            "DatabricksDataSourceImpl = soda_databricks.common.data_sources.databricks_data_source:DatabricksDataSourceImpl",
        ],
    },
)
