#!/usr/bin/env python

from setuptools import setup

package_name = "soda-oracle"
package_version = "4.0.0b10"
description = "Soda oracle V4"

requires = [
    f"soda-core=={package_version}",
    "oracledb>=2.4.1",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.oracle": [
            "OracleDataSourceImpl = soda_oracle.common.data_sources.oracle_data_source:OracleDataSourceImpl",
        ],
    },
)
