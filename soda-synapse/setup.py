#!/usr/bin/env python

from setuptools import setup

package_name = "soda-synapse"
package_version = "4.0.0b11"
description = "Soda Synapse V4"

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
        "soda.plugins.data_source.synapse": [
            "SynapseDataSourceImpl = soda_synapse.common.data_sources.synapse_data_source:SynapseDataSourceImpl",
        ],
    },
)
