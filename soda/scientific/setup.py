#!/usr/bin/env python
import sys

from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 8):
    print("Error: Soda SQL requires at least Python 3.8")
    print("Error: Please upgrade your Python version to 3.8 or later")
    sys.exit(1)

package_name = "soda-core-scientific"
package_version = "3.0.0b19"
description = "Soda Core Scientific Package"
requires = [
    f"soda-core=={package_version}",
    "wheel",
    "pydantic>=1.8.1,<2.0.0",
    "scipy>=1.8.0",
    "inflection==0.5.1",
    "httpx>=0.18.1,<2.0.0",
    "PyYAML>=5.4.1,<6.0.0",
    "cython>=0.22",
    "pystan==2.19.1.1",
    "prophet>=1.0.0"
    # Uncomment & recreate .venv to make scientific tests work
    # "prophet @ git+https://github.com/facebook/prophet.git#egg=prophet&subdirectory=python",
]

# TODO Fix the params
setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
    package_data={
        "": ["detector_config.yaml"],
    },
)
