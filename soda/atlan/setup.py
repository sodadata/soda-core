#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-atlan"
package_version = "3.5.6"
description = "Soda Core Atlan Package"

requires = [f"soda-core=={package_version}", "pyatlan>=2.2.4, <3.0"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
