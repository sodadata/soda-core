#!/usr/bin/env python

from setuptools import find_namespace_packages, setup

package_name = "soda-core-scientific"
package_version = "3.3.4"
description = "Soda Core Scientific Package"
requires = [
    f"soda-core=={package_version}",
    "pandas<2.0.0",
    "wheel",
    "pydantic>=2.0.0, <3.0.0",
    "scipy>=1.8.0",
    "numpy>=1.23.3, <2.0.0",
    "inflection==0.5.1",
    "httpx>=0.18.1,<2.0.0",
    "PyYAML>=5.4.1,<7.0.0",
    "cython>=0.22",
    "prophet>=1.1.5,<2.0.0",
]

simulator_deps = [
    "streamlit>=1.30.0,<2.0.0",
    "plotly>=5.18.0",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    packages=find_namespace_packages(include=["soda*"]),
    package_data={
        "": ["detector_config.yaml"],
        "soda.scientific.anomaly_detection_v2.simulate": ["assets/*"],
    },
    extras_require={
        "simulator": simulator_deps,
    },
)
