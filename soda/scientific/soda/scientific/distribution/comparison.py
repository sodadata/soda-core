import abc
import logging
from typing import Any, List, Tuple

import numpy as np
import pandas as pd
import yaml
from pydantic import FilePath
from scipy.stats import chisquare, ks_2samp
from soda.sodacl.distribution_check_cfg import DistributionCheckCfg

from soda.scientific.distribution.generate_dro import DROGenerator
from soda.scientific.distribution.utils import (
    DistCfg,
    RefDataCfg,
    assert_bidirectional_categorial_values,
    assert_categorical_min_sample_size,
    distribution_is_all_null,
)


class NotEnoughSamplesException(Exception):
    """Thrown when inssuficient samples-like events are detected."""


class DistributionRefKeyException(Exception):
    """Thrown when ref key parsing fails"""


class DistributionRefParsingException(Exception):
    """Thrown when ref yaml file parsing fails"""


class MissingCategories(Exception):
    """Thrown when a category in the test data is missing from the ref data."""


class DistributionChecker:
    def __init__(self, distribution_check_cfg: DistributionCheckCfg, data: List[Any]):
        cfg = DistCfg(
            reference_file_path=distribution_check_cfg.reference_file_path,
        )
        self.test_data = data

        self.ref_cfg = self._parse_reference_cfg(cfg.reference_file_path)

        algo_mapping = {
            "categorical": ChiSqAlgorithm,
            "continuous": KSAlgorithm,
            "chi_square": ChiSqAlgorithm,
            "ks": KSAlgorithm,
        }
        self.choosen_algo = algo_mapping[self.ref_cfg.method]

    def run(self) -> Tuple[float, float]:
        test_data = pd.Series(self.test_data)

        bootstrap_size = 10
        stat_values = []
        p_values = []

        for i in range(bootstrap_size):
            stat_value, p_value = self.choosen_algo(self.ref_cfg, test_data, seed=i).evaluate()
            stat_values.append(stat_value)
            p_values.append(p_value)

        stat_value = np.median(stat_values)
        p_value = np.median(p_values)

        return stat_value, p_value

    def _parse_reference_cfg(self, ref_file_path: FilePath) -> RefDataCfg:
        with open(str(ref_file_path)) as stream:
            try:
                parsed_file: dict = yaml.safe_load(stream)
                ref_data_cfg = {}
                if "method" in parsed_file:
                    ref_data_cfg["method"] = parsed_file["method"]
                else:
                    raise DistributionRefKeyException(
                        f"Your {ref_file_path} reference yaml file must have `method` key"
                    )

                if "distribution reference" in parsed_file:
                    # TODO: add checks for bins and weights
                    ref_data_cfg["bins"] = parsed_file["distribution reference"]["bins"]
                    ref_data_cfg["weights"] = parsed_file["distribution reference"]["weights"]

                else:
                    dro = DROGenerator(cfg=RefDataCfg(method=ref_data_cfg["method"]), data=self.test_data).generate()
                    ref_data_cfg["bins"] = dro.bins
                    ref_data_cfg["weights"] = dro.weights

            except yaml.YAMLError as exc:
                logging.error(exc)
                raise DistributionRefParsingException(
                    f"Cannot parse {ref_file_path}, please check your reference file! \n"
                )
        return RefDataCfg.parse_obj(ref_data_cfg)


class DistributionAlgorihm:
    def __init__(self, cfg: RefDataCfg, test_data: pd.Series, seed: int = 61) -> None:
        self.cfg = cfg
        self.test_data = test_data
        self.rng = np.random.default_rng(seed)
        self.ref_data = self.generate_ref_data()

    @abc.abstractmethod
    def evaluate(self, test_data: pd.Series) -> Tuple[float, float]:
        ...

    @abc.abstractmethod
    def generate_ref_data(self) -> pd.Series:
        ...


class ChiSqAlgorithm(DistributionAlgorihm):
    def __init__(self, cfg: RefDataCfg, test_data: pd.Series, seed: int = 61) -> None:
        super().__init__(cfg, test_data, seed)

    def evaluate(self) -> Tuple[float, float]:
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
        return stat_value, p_value

    def generate_ref_data(self) -> pd.Series:
        sample_size = len(self.test_data)
        return pd.Series(self.rng.choice(self.cfg.bins, p=self.cfg.weights, size=sample_size))


class KSAlgorithm(DistributionAlgorihm):
    def __init__(self, cfg: RefDataCfg, test_data: pd.Series, seed: int = 61) -> None:
        super().__init__(cfg, test_data, seed)

    def evaluate(self) -> Tuple[float, float]:
        # TODO: set up some assertion testing that the dtypes are continuous
        # TODO: consider whether we may want to warn users if any or both of their series are nulls
        # although ks_2samp() behaves correctly in either cases
        stat_value, p_value = ks_2samp(self.ref_data, self.test_data)
        return stat_value, p_value

    def generate_ref_data(self) -> pd.Series:
        sample_size = len(self.test_data)
        sample_data = self.rng.random((1, sample_size))[0]
        xp = np.cumsum(self.cfg.weights)
        yp = self.cfg.bins
        ref_data = np.interp(sample_data, xp, yp)
        return ref_data
