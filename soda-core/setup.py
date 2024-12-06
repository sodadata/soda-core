#!/usr/bin/env python

from setuptools import setup

package_name = "soda-core"
package_version = "4.0.0b1"
description = "Soda Core V4"

requires = [
    "jsonschema>=4.20.0",
    "ruamel.yaml>=0.17.0,<0.18.0",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    package_data={"": ["**/*.json"]},
    include_package_data=True
)
