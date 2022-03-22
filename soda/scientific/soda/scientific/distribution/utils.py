from typing import List, Optional, Sequence

import numpy as np
import pandas as pd
from pydantic import BaseModel, FilePath, validator


class DistCfg(BaseModel):
    """Validation model for configuration."""

    reference_file_path: Optional[FilePath] = None


class RefDataCfg(BaseModel):
    """Validation model for reference data configuration."""

    bins: Optional[List]
    weights: Optional[List[float]]
    labels: Optional[List]
    method: str

    @validator("weights")
    def check_weights_sum(cls, v):
        _sum = np.sum(v)
        np.testing.assert_almost_equal(
            actual=_sum, desired=1, decimal=3, err_msg=f"The sum of the weights should 1! The current sum is '{_sum}'"
        )
        return v

    @validator("method")
    def check_accepted_values_method(cls, v):
        valid_distribution_methods = ["categorical", "continuous", "ks", "chi_square"]
        assert (
            v in valid_distribution_methods
        ), f"Method must be one of {valid_distribution_methods}, but '{v}' was provided."
        return v


def assert_categorical_min_sample_size(
    value_counts: pd.Series, min_n_values: int, comparison_method: str
) -> Optional[str]:
    if min(value_counts) < min_n_values:
        return (
            "The following categories in the reference data are below {min_n_values} observations:\n"
            f"{value_counts.index[value_counts.values < min_n_values].values}\n"  # type: ignore
            f"A {comparison_method} test requires a minimum of {min_n_values} observations per categories\n"
        )
    return None


def assert_bidirectional_categorial_values(
    ref_data_frequencies: pd.Series, test_data_frequencies: pd.Series
) -> Sequence[Optional[str]]:
    missing_categories_issues = []
    if set(ref_data_frequencies.index).difference(test_data_frequencies.index):
        missing_categories_issues.append(
            "The following categories in your **test data** are not represented in your "
            f"**reference data**:\n {set(ref_data_frequencies.index).difference(test_data_frequencies.index)}"
        )
    if set(test_data_frequencies.index).difference(ref_data_frequencies.index):
        missing_categories_issues.append(
            "The following categories in your **reference data** are not represented in your "
            f"**test data**: \n {set(test_data_frequencies.index).difference(ref_data_frequencies.index)}"
        )
    return missing_categories_issues


def distribution_is_all_null(distribution: pd.Series) -> bool:
    if pd.isnull(distribution).all():
        return True
    else:
        return False
