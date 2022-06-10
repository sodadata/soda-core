import abc
import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
import yaml
from pydantic import FilePath
from scipy.stats import chisquare, ks_2samp, wasserstein_distance
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg

from soda.scientific.distribution.generate_dro import DROGenerator
from soda.scientific.distribution.utils import (
    DistCfg,
    RefDataCfg,
    assert_bidirectional_categorial_values,
    assert_categorical_min_sample_size,
    distribution_is_all_null,
    generate_ref_data,
)


class NotEnoughSamplesException(Exception):
    """Thrown when inssuficient samples-like events are detected."""


class DistributionRefKeyException(Exception):
    """Thrown when ref key parsing fails"""


class DistributionRefParsingException(Exception):
    """Thrown when ref yaml file parsing fails"""


class MissingCategories(Exception):
    """Thrown when a category in the test data is missing from the ref data."""


class DistributionRefIncompatibleException(Exception):
    """Thrown when the DRO distribution_type is incompatible with the test that is used."""


class DistributionChecker:
    def __init__(self, distribution_check_cfg: DistributionCheckCfg, data: List[Any]):
        cfg = DistCfg(
            reference_file_path=distribution_check_cfg.reference_file_path,
        )
        self.test_data = data

        self.method = distribution_check_cfg.method
        self.ref_cfg = self._parse_reference_cfg(cfg.reference_file_path)

        algo_mapping = {
            "chi_square": ChiSqAlgorithm,
            "ks": KSAlgorithm,
            "swd": SWDAlgorithm,
            "semd": SWDAlgorithm,
            "psi": PSIAlgorithm,
        }
        self.choosen_algo = algo_mapping.get(self.method)

    def run(self) -> Dict[str, float]:
        test_data = pd.Series(self.test_data)

        bootstrap_size = 10
        check_values = []
        stat_values = []

        for i in range(bootstrap_size):
            check_results = self.choosen_algo(self.ref_cfg, test_data, seed=i).evaluate()

            check_value = check_results.get("check_value")
            check_values.append(check_value)

            stat_value = check_results.get("stat_value")
            if stat_value:
                stat_values.append(stat_value)

        check_value = np.median(check_values)
        if stat_values:
            stat_value = np.median(stat_values)

        return dict(check_value=check_value, stat_value=stat_value)

    def _parse_reference_cfg(self, ref_file_path: FilePath) -> RefDataCfg:
        with open(str(ref_file_path)) as stream:
            try:
                parsed_file: dict = yaml.safe_load(stream)
                ref_data_cfg = {}

                if "distribution_type" in parsed_file:
                    ref_data_cfg["distribution_type"] = parsed_file["distribution_type"]
                else:
                    raise DistributionRefKeyException(
                        f"Your {ref_file_path} reference yaml file must have `distribution_type` key. The `distribution_type` is used to create a sample from your DRO."
                        f" For more information visit the docs: https://docs.soda.io/soda-cl/distribution.html#generate-a-distribution-reference-object-dro"
                    )

                if not self.method:
                    default_configs = {"continuous": "ks", "categorical": "chi_square"}
                    self.method = default_configs[ref_data_cfg["distribution_type"]]

                correct_configs = {
                    "continuous": ["ks", "psi", "swd", "semd"],
                    "categorical": ["chi_square", "psi", "swd", "semd"],
                }

                if self.method not in correct_configs[ref_data_cfg["distribution_type"]]:
                    raise DistributionRefIncompatibleException(
                        f"""Your DRO distribution_type '{parsed_file['distribution_type']}' is incompatible with the method '{self.method}'. Your DRO distribution_type allows you to use one of the following methods:"""
                        f""" {", ".join([f"'{method}'" for method in correct_configs[parsed_file["distribution_type"]]])}. For more information visit the docs: https://docs.soda.io/soda-cl/distribution.html#about-distribution-checks """
                    )

                if "distribution reference" in parsed_file:
                    # TODO: add checks for bins and weights
                    ref_data_cfg["bins"] = parsed_file["distribution reference"]["bins"]
                    ref_data_cfg["weights"] = parsed_file["distribution reference"]["weights"]

                else:
                    dro = DROGenerator(
                        cfg=RefDataCfg(distribution_type=ref_data_cfg["distribution_type"]), data=self.test_data
                    ).generate()
                    ref_data_cfg["bins"] = dro.bins
                    ref_data_cfg["weights"] = dro.weights

            except yaml.YAMLError as exc:
                logging.error(exc)
                raise DistributionRefParsingException(
                    f"Cannot parse {ref_file_path}, please check your reference file! \n"
                )
        return RefDataCfg.parse_obj(ref_data_cfg)


class DistributionAlgorithm(abc.ABC):
    def __init__(self, cfg: RefDataCfg, test_data: pd.Series, seed: int = 61) -> None:
        self.test_data = test_data
        self.ref_data = generate_ref_data(cfg, len(test_data), np.random.default_rng(seed))

    @abc.abstractmethod
    def evaluate(self) -> Dict[str, float]:
        ...


class ChiSqAlgorithm(DistributionAlgorithm):
    def evaluate(self) -> Dict[str, float]:
        # TODO: make sure we can assert we're really dealing with categories
        # TODO: make sure that we also can guarantee the order of the categorical labels
        # since we're comparing on indeces in the chisquare function
        assert not distribution_is_all_null(self.ref_data), "Reference data cannot contain only null values"
        assert not distribution_is_all_null(self.test_data), "Test data cannot contain only null values"

        ref_data_frequencies = self.ref_data.value_counts()
        test_data_frequencies = self.test_data.value_counts()

        # check that all categories in test are present in the reference data and vice versa
        missing_categories_issues = assert_bidirectional_categorial_values(ref_data_frequencies, test_data_frequencies)
        if missing_categories_issues:
            missing_categories_issues_message = "\n".join(list(filter(None, missing_categories_issues)))
            raise MissingCategories(
                f"All categories are not both in your test and reference data: \n {missing_categories_issues_message}"
            )

        # check that all observed and expected freqs are at least 5
        # as per: https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.chisquare.html
        sample_n_issues = []
        sample_n_issues.append(
            assert_categorical_min_sample_size(
                value_counts=ref_data_frequencies, min_n_values=5, comparison_method="Chi Square"
            )
        )
        sample_n_issues.append(
            assert_categorical_min_sample_size(
                value_counts=test_data_frequencies, min_n_values=5, comparison_method="Chi Square"
            )
        )
        sample_n_issues = list(filter(None, sample_n_issues))
        if len(sample_n_issues) > 0:
            sample_n_issues_message = "\n".join(sample_n_issues)
            raise NotEnoughSamplesException(f"There were issues with your data:\n{sample_n_issues_message}")

        # add 1 to all categories to avoid 0 div issues when normalising
        ref_data_frequencies = ref_data_frequencies + 1
        test_data_frequencies = test_data_frequencies + 1

        # Normalise because scipy wants sums of observed and reference counts to be equal
        # workaround found and discussed in: https://github.com/UDST/synthpop/issues/75#issuecomment-907137304
        stat_value, p_value = chisquare(
            test_data_frequencies,
            ref_data_frequencies * np.mean(test_data_frequencies) / np.mean(ref_data_frequencies),
        )
        return dict(stat_value=stat_value, check_value=p_value)


class KSAlgorithm(DistributionAlgorithm):
    def evaluate(self) -> Dict[str, float]:
        # TODO: set up some assertion testing that the distribution_type are continuous
        # TODO: consider whether we may want to warn users if any or both of their series are nulls
        # although ks_2samp() behaves correctly in either cases
        stat_value, p_value = ks_2samp(self.ref_data, self.test_data)
        return dict(stat_value=stat_value, check_value=p_value)


class SWDAlgorithm(DistributionAlgorithm):
    def evaluate(self) -> Dict[str, float]:
        wd = wasserstein_distance(self.ref_data, self.test_data)
        swd = wd / np.std(np.concatenate([self.ref_data, self.test_data]))
        return dict(check_value=swd)


class PSIAlgorithm(DistributionAlgorithm):
    def evaluate(self) -> Dict[str, float]:
        max_val = max(np.max(self.test_data), np.max(self.ref_data))
        min_val = min(np.min(self.test_data), np.min(self.ref_data))
        bins = np.linspace(min_val, max_val, 11)

        hist_a = self.construct_hist(self.test_data, bins)
        hist_b = self.construct_hist(self.ref_data, bins)

        psi = np.sum((hist_a - hist_b) * np.log((hist_a) / (hist_b)))
        return dict(check_value=psi)

    @staticmethod
    def construct_hist(a: pd.Series, bins: np.ndarray) -> np.ndarray:
        hist_a, _ = np.histogram(a, bins=bins)
        hist_a = hist_a / len(a)
        small_num = 10**-5
        hist_a += np.where(hist_a == 0, small_num, 0)
        return hist_a
