#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-teradata"
package_version = "3.3.14"
description = "Soda Core Teradata Package"

requires = [
    f"soda-core=={package_version}",
    "teradatasql>=17.10.0.0",
]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
