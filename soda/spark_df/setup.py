#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-spark-df"
package_version = "3.5.6"
description = "Soda Core Spark Dataframe Package"

requires = [
    f"soda-core-spark=={package_version}",
]
# pyspark is an extra so databricks-connect, which ships its own pyspark module, can stand in for it.
# 4.0.0 has breaking changes, drops java 11 support. Revisit as needed.
extras = {"pyspark": ["pyspark>=3.5.0,<4.0.0"]}
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    extras_require=extras,
    packages=find_namespace_packages(include=["soda*"]),
)
