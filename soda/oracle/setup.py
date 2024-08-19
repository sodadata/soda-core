#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-oracle"
package_version = "3.3.14"
# TODO Add proper description
description = "Soda Core Oracle Package"

requires = [f"soda-core=={package_version}", "oracledb>=1.1.1,<3.0.0"]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
