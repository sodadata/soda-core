#!/usr/bin/env python

import sys

from setuptools import setup

if sys.version_info < (3, 9):
    print("Error: soda-core requires at least Python 3.9")
    print("Error: Please upgrade your Python version to 3.9 or later")
    sys.exit(1)

package_name = "soda-core"
package_version = "4.0.0b8"
description = "Soda Core V4"

requires = [
    "ruamel.yaml>=0.17.0,<0.18.0",
    "requests>=2.32.3,<2.33.0",
    "pydantic>=2.0,<3.0",
    "opentelemetry-api>=1.16.0,<2.0.0",
    "opentelemetry-exporter-otlp-proto-http>=1.16.0,<2.0.0",
    "tabulate[widechars]",
    "python-dotenv~=1.0",
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
    entry_points={
        "console_scripts": [
            "soda=soda_core.cli.cli:execute",
        ]
    },
)
