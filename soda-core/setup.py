#!/usr/bin/env python

import sys

from setuptools import setup

if sys.version_info < (3, 9):
    print("Error: soda-core requires at least Python 3.9")
    print("Error: Please upgrade your Python version to 3.9 or later")
    sys.exit(1)

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
    author="Soda Data N.V.",
    author_email="info@soda.io",
    description="Soda core library & CLI",
    package_dir={"": "src"},
    package_data={"": ["**/*.json"]},
    include_package_data=True,
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.10",
    ],
)
