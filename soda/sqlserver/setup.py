#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-sqlserver"
package_version = "3.4.2"
description = "Soda Core SQL Server Package"

requires = [f"soda-core=={package_version}", "pyodbc", "azure-identity~=1.17.1"]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
