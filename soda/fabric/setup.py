#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-fabric"
package_version = "3.2.3"
description = "Soda Core Fabric Package"

requires = [f"soda-core=={package_version}", "azure-identity", "pyodbc", "faker"]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
