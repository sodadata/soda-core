#!/usr/bin/env python

from setuptools import setup

package_name = "soda-athena"
package_version = "4.0.5b1"
description = "Soda Athena V4"

requires = [f"soda-core=={package_version}", "PyAthena>=2.2.0, <3.0"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.athena": [
            "AthenaDataSourceImpl = soda_athena.common.data_sources.athena_data_source:AthenaDataSourceImpl",
        ],
    },
)
