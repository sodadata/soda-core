#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-impala"
package_version = "3.5.5"
description = "Soda Core Impala Package"

requires = [f"soda-core=={package_version}", "impyla==0.19.0"]
# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
