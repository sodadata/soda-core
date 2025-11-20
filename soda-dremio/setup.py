#!/usr/bin/env python

from setuptools import setup

package_name = "soda-dremio"
package_version = "4.0.0b12"
description = "Soda Dremio V4"

requires = [
    f"soda-core=={package_version}",
    "adbc-driver-flightsql>=1.0.0",
    "pyarrow>=14.0.0",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.dremio": [
            "DremioDataSourceImpl = soda_dremio.common.data_sources.dremio_data_source:DremioDataSourceImpl",
        ],
    },
)
