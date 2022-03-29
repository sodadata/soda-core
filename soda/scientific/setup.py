#!/usr/bin/env python
import sys

from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 7):
    print("Error: Soda SQL requires at least Python 3.7")
    print("Error: Please upgrade your Python version to 3.7 or later")
    sys.exit(1)

package_name = "soda-core-scientific"
package_version = "3.0.0b5"
description = "Soda Core Scientific Package"

requires = [
    f"soda-core=={package_version}",
    "u8darts==0.7.0",
    "pydantic>=1.8.1,<2.0.0" "inflection==0.5.1",
    "httpx>=0.18.1,<2.0.0",
    "PyYAML>=5.4.1,<6.0.0",
    "numpy>=1.19.5,<2.0.0",
]

# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
)
