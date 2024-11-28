#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-spark-df"
package_version = "3.4.2"
description = "Soda Core Spark Dataframe Package"

requires = [
    f"soda-core-spark=={package_version}",
    "pyspark>=3.4.0",
]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
