#!/usr/bin/env python

from setuptools import setup

package_name = "soda-fabric"
package_version = "4.0.4b22"
description = "Soda Fabric V4"

requires = [
    f"soda-core=={package_version}",
    f"soda-sqlserver=={package_version}",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.fabric": [
            "FabricDataSourceImpl = soda_fabric.common.data_sources.fabric_data_source:FabricDataSourceImpl",
        ],
    },
)
