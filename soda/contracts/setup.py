#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-contracts"
package_version = "3.2.2"
description = "Soda Core Contracts Package"

requires = [
    f"soda-core=={package_version}",
]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
