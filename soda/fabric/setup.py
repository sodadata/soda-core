#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-fabric"
package_version = "3.5.3"
description = "Soda Core Microsoft Fabric Package"

requires = [f"soda-core-sqlserver=={package_version}"]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
