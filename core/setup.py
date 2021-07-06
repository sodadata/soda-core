#!/usr/bin/env python
import sys
import pathlib
from setuptools import setup, find_namespace_packages

if sys.version_info < (3, 7):
    print('Error: Soda SQL requires at least Python 3.7')
    print('Error: Please upgrade your Python version to 3.7 or later')
    sys.exit(1)

package_name = "soda-sql-core"
# Managed by tbump - don't change manually
# And we can't have nice semver (<major>.<minor>.<patch>-<pre-release>-<build>)
# like "-alpha-1" as long as this is open >> https://github.com/pypa/setuptools/issues/2181
package_version = '2.1.0b11'
description = "Soda SQL Core"

long_description = (pathlib.Path(__file__).parent / "README.md").read_text()

requires = [
    'Jinja2>=2.11.3',
    'click>=7.1.2',
    'cryptography>=3.3.2',
    'pyyaml>=5.4.1',
    'requests>=2.23.0'
]
# TODO Fix the params
# TODO Add a warning that installing core doesn't give any warehouse functionality
setup(
    name=package_name,
    version=package_version,
    author="Tom Baeyens",
    author_email="tom@soda.io",
    description="Soda SQL library & CLI",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_namespace_packages(include=["sodasql*"]),
    install_requires=requires,
    entry_points={
        "console_scripts":
            [
                "soda=sodasql.cli.cli:main"
            ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.7",
)
