#!/usr/bin/env python
import sys

from setuptools import find_namespace_packages, setup

if sys.version_info < (3, 8):
    print("Error: Soda SQL requires at least Python 3.8")
    print("Error: Please upgrade your Python version to 3.8 or later")
    sys.exit(1)

package_name = "soda-core-scientific"
package_version = "3.0.0b11"
description = "Soda Core Scientific Package"
requires = [
    f"soda-core=={package_version}",
    "u8darts>=0.7.0,<1.0.0",
    "pydantic>=1.8.1,<2.0.0",
    "inflection==0.5.1",
    "httpx>=0.18.1,<2.0.0",
    "PyYAML>=5.4.1,<6.0.0",
    # The following incantation and Python 3.8 is neded to make stuff work.
    # Sometimes in life, universe (by that I mean python) throws sh*t at you
    # and you think you can reason with Python and PiP and try to solve a "problem"
    # ... but you would be wrong.
    # The best course of action is acceptance, despite the horrors you see in setup.py files
    # such as this. And that my dear reader of the code is the real 'Zen of Python`.
    "prophet @ git+https://github.com/facebook/prophet.git#egg=prophet&subdirectory=python",
    # "prophet",
    "pystan==2.19.1.1",
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
