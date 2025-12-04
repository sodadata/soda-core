#!/usr/bin/env python

from setuptools import setup

package_name = "soda-redshift"
package_version = "4.0.4b19"
description = "Soda Redshift V4"

requires = [f"soda-core=={package_version}", "psycopg2-binary>=2.8.5, <3.0", "boto3"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.redshift": [
            "RedshiftDataSourceImpl = soda_redshift.common.data_sources.redshift_data_source:RedshiftDataSourceImpl",
        ],
    },
)
