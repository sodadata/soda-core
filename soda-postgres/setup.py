#!/usr/bin/env python

from setuptools import setup

package_name = "soda-postgres"
package_version = "4.0.0b1"
description = "Soda Postgres V4"

requires = [
    f"soda-core=={package_version}",
    "psycopg2-binary>=2.8.5, <3.0"
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"}
)