#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-contracts"
package_version = "3.3.0"
description = "Soda Core Contracts Package"

requires = [f"soda-core=={package_version}", "jsonschema>=4.20.0"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
    package_data={"": ["*.json"]},
    include_package_data=True,
)
