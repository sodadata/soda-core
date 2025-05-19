#!/usr/bin/env python

import sys

from setuptools import setup

if sys.version_info < (3, 9):
    print("Error: soda-core requires at least Python 3.9")
    print("Error: Please upgrade your Python version to 3.9 or later")
    sys.exit(1)

package_name = "soda-tests"
package_version = "4.0.0b1"
description = "Soda Core V4 Tests"

requires = [
    f"soda-core=={package_version}",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    description="Soda Core Tests",
    package_dir={"": "src"},
    package_data={"": ["**/*.json"]},
    include_package_data=True,
    python_requires=">=3.9",
)
