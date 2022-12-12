#!/usr/bin/env python
import sys

from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 7):
    print("Error: Soda Core requires at least Python 3.7")
    print("Error: Please upgrade your Python version to 3.7 or later")
    sys.exit(1)

package_name = "soda-core"
# Managed by tbump - do not change manually
package_version = "3.0.15"
description = "Soda Core"

# long_description = (pathlib.Path(__file__).parent.parent / "README.md").read_text()

requires = [
    "markupsafe==2.0.1",
    "Jinja2~=2.11",  # Downgraded from 3.x until dbt-core 1.3 is released
    "click~=8.0",
    "ruamel.yaml~=0.17.21",
    "requests~=2.27",
    "antlr4-python3-runtime~=4.11.1",
    "opentelemetry-api~=1.11.0",
    "opentelemetry-exporter-otlp-proto-http~=1.11.0",
    "protobuf~=3.19.0",
    "sqlparse~=0.4",
]

setup(
    name=package_name,
    version=package_version,
    author="Soda Data N.V.",
    author_email="info@soda.io",
    description="Soda Core library & CLI",
    long_description="long_description",  # TODO: fix this as it fails in tox
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(include=["soda*"]),
    install_requires=requires,
    entry_points={"console_scripts": ["soda=soda.cli.cli:main"]},
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.7",
)
