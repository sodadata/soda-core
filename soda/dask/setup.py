#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-pandas-dask"
package_version = "3.3.18"
description = "Soda Core Dask Package"

# 2023.10 or its subdependencies introduces breaking changes in how rows are counted, so we stay away from it for now.
requires = [f"soda-core=={package_version}", "dask>=2022.10.0", "dask-sql>=2022.12.0,<2023.10.0"]


setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
