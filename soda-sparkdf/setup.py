#!/usr/bin/env python

from setuptools import setup

package_name = "soda-sparkdf"
package_version = "4.0.0b13"
description = "Soda SparkDF V4"

requires = [f"soda-core=={package_version}", "freezegun", "pyspark>=3.5.0", f"soda-databricks=={package_version}"]
# Note: some java runtime (>17) is required to run the tests.

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.sparkdf": [
            "SparkDataFrameDataSourceImpl = soda_sparkdf.common.data_sources.sparkdf_data_source:SparkDataFrameDataSourceImpl",
        ],
    },
)
