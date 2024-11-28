#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-athena"
package_version = "3.4.2"
description = "Soda Core Athena Package"

requires = [
    f"soda-core=={package_version}",
    "PyAthena>=2.2.0, <3.0",
]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
