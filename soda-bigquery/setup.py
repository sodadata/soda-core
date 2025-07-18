#!/usr/bin/env python

from setuptools import setup

package_name = "soda-bigquery"
package_version = "4.0.0b7"
description = "Soda BigQuery V4"

requires = [
    f"soda-core=={package_version}",
    "google-cloud-bigquery>=2.25.0",
]

setup(
    name=package_name,
    version=package_version,
    install_requires=requires,
    package_dir={"": "src"},
    entry_points={
        "soda.plugins.data_source.bigquery": [
            "BigqueryDataSourceImpl = soda_bigquery.common.data_sources.bigquery_data_source:BigQueryDataSourceImpl",
        ],
    },
)
