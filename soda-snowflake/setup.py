#!/usr/bin/env python

from setuptools import setup

package_name = "soda-snowflake"
package_version = "4.0.0b7"
description = "Soda Snowflake V4"

requires = [f"soda-core=={package_version}", "snowflake-connector-python~=3.0"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.snowflake": [
            "SnowflakeDataSourceImpl = soda_snowflake.common.data_sources.snowflake_data_source:SnowflakeDataSourceImpl",
        ],
    },
)
