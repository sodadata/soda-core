import numpy as np
import pandas as pd
import pytest
from numpy.random import default_rng

from soda.scientific.distribution.comparison import (
    DistributionRefKeyException,
    DistributionRefParsingException,
)
from soda.scientific.distribution.utils import DistCfg, RefDataCfg


@pytest.mark.parametrize(
    "method",
    [
        pytest.param("continuous", id="valid method continuous"),
        pytest.param("categorical", id="valid method categorical"),
        pytest.param("ks", id="valid method ks"),
        pytest.param("chi_square", id="valid method chi_square"),
        pytest.param("trabzonspor", id="invalid method"),
    ],
)
def test_config_method(method):
    from pydantic.error_wrappers import ValidationError

    try:
        bins = [1, 2, 3]
        weights = [0.1, 0.8, 0.1]
        RefDataCfg(bins=bins, weights=weights, labels=None, method=method)
    except ValidationError:
        pass


@pytest.mark.parametrize(
    "weights",
    [
        pytest.param([0.5, 0.3, 0.2], id="valid weights with sum == 1"),
        pytest.param([0.5, 0.5, 0.5], id="invalid weights with sum != 1"),
        pytest.param([None, 0.5, 0.5], id="invalid weights with sum == 1 but having none"),
    ],
)
def test_config_weights(weights):
    from pydantic.error_wrappers import ValidationError

    try:
        bins = [1, 2, 3]
        method = "continuous"
        RefDataCfg(bins=bins, weights=weights, labels=None, method=method)
    except ValidationError:
        pass


@pytest.mark.parametrize(
    "reference_file_path",
    [
        pytest.param("LICENSE", id="Valid file that exists"),
        pytest.param("1967.txt", id="Invalid file that doesn't exists"),
    ],
)
def test_ref_file_path(reference_file_path):
    from pydantic.error_wrappers import ValidationError

    try:
        DistCfg(reference_file_path=reference_file_path)
    except ValidationError:
        pass


@pytest.mark.parametrize(
    "reference_file_path, test_data, expected_stat, expected_p",
    [
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            list(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.036,
            0.5545835690881001,
            id="Similar continuous distribution",
        ),
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            list(default_rng(61).normal(loc=1.5, scale=1.0, size=1000)),
            0.2115,
            8.845435165401255e-20,
            id="Different continuous distribution",
        ),
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_continuous_no_bins.yml",
            list(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.0245,
            0.9211961644657093,
            id="Similar continuous distribution without bins and weights",
        ),
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_categorical_no_bins.yml",
            ["peace", "at", "home", "peace", "in", "the", "world"] * 1000,
            2.849628571261552,
            0.7222714008190096,
            id="Similar categorical distribution without bins and weights",
        ),
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_categorical.yml",
            [1, 1, 2, 3] * 1000,
            156.32764391336602,
            1.960998922048572e-34,
            id="Different categorical distribution",
        ),
    ],
)
def test_distribution_checker(reference_file_path, test_data, expected_stat, expected_p):
    from soda.scientific.distribution.comparison import DistributionChecker

    test_dist_cfg = DistCfg(reference_file_path=reference_file_path)
    check = DistributionChecker(test_dist_cfg, test_data)
    stat_val, p_val = check.run()
    assert stat_val == pytest.approx(expected_stat, abs=1e-3)
    assert p_val == pytest.approx(expected_p, abs=1e-3)


@pytest.mark.parametrize(
    "reference_file_path, exception",
    [
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_missing_method.yml",
            DistributionRefKeyException,
            id="Missing key method",
        ),
        pytest.param(
            "soda/scientific/tests/assets/invalid.yml",
            DistributionRefParsingException,
            id="Corrupted yaml file",
        ),
    ],
)
def test_ref_config_file_exceptions(reference_file_path, exception):
    from soda.scientific.distribution.comparison import DistributionChecker

    with pytest.raises(exception):
        test_data = list(pd.Series(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)))
        test_dist_cfg = DistCfg(reference_file_path=reference_file_path)
        DistributionChecker(test_dist_cfg, test_data)


@pytest.mark.parametrize(
    "reference_file_path, expected_stat, expected_p",
    [
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_continuous_no_bins.yml",
            0.0245,
            0.9211961644657093,
            id="Missing bins and weights",
        ),
    ],
)
def test_with_no_bins_and_weights(reference_file_path, expected_stat, expected_p):
    from soda.scientific.distribution.comparison import DistributionChecker

    test_dist_cfg = DistCfg(reference_file_path=reference_file_path)
    test_data = list(default_rng(61).normal(loc=1.0, scale=1.0, size=1000))
    stat_val, p_val = DistributionChecker(test_dist_cfg, test_data).run()
    assert stat_val == pytest.approx(expected_stat, abs=1e-3)
    assert p_val == pytest.approx(expected_p, abs=1e-3)


# The following bins and weights are generated based on
# default_rng().normal(loc=1.0, scale=1.0, size=1000)
TEST_CONFIG_CONT_1 = RefDataCfg(
    bins=[
        -3.34034354,
        -3.09007903,
        -2.83981452,
        -2.58955002,
        -2.33928551,
        -2.089021,
        -1.8387565,
        -1.58849199,
        -1.33822748,
        -1.08796298,
        -0.83769847,
        -0.58743396,
        -0.33716946,
        -0.08690495,
        0.16335956,
        0.41362406,
        0.66388857,
        0.91415308,
        1.16441758,
        1.41468209,
        1.6649466,
        1.9152111,
        2.16547561,
        2.41574012,
        2.66600462,
        2.91626913,
        3.16653364,
        3.41679815,
        3.66706265,
        3.91732716,
        4.16759167,
        4.41785617,
        4.66812068,
        4.91838519,
    ],
    weights=[
        0,
        0.001,
        0.0,
        0.0,
        0.0,
        0.001,
        0.0,
        0.001,
        0.005,
        0.008,
        0.014,
        0.021,
        0.034,
        0.048,
        0.059,
        0.077,
        0.094,
        0.104,
        0.097,
        0.102,
        0.094,
        0.058,
        0.051,
        0.051,
        0.028,
        0.02,
        0.016,
        0.007,
        0.004,
        0.001,
        0.001,
        0.0,
        0.0,
        0.003,
    ],
    labels=None,
    method="continuous",
)


@pytest.mark.parametrize(
    "test_data, expectated_stat_val, expected_p_val",
    [
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.046,
            0.24068202486600215,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=0.9, size=1000)),
            0.064,
            0.033253124816560224,
            id="distributions are different_1",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=0.9, scale=1.0, size=1000)),
            0.078,
            0.004543821879051605,
            id="distributions are different_2",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=10, scale=10, size=1000)),
            0.735,
            1.3332739479484995e-262,
            id="distributions are extremely different",
        ),
        pytest.param(
            pd.Series(np.full([100], np.nan)),
            1.0,
            0.0,
            id="distributions are all made of nulls",
        ),
    ],
)
def test_continuous_comparison(test_data, expectated_stat_val, expected_p_val):
    from soda.scientific.distribution.comparison import KSAlgorithm

    stat_val, p_val = KSAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert expectated_stat_val == pytest.approx(stat_val, abs=1e-3)
    assert expected_p_val == pytest.approx(p_val, abs=1e-3)


# The following bins and weights are generated based on
# default_rng().normal(loc=1.0, scale=1.0, size=1000)
TEST_CONFIG_CATEGORIC_1 = RefDataCfg(bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, method="categorical")


@pytest.mark.parametrize(
    "test_data, expectated_stat_val, expected_p_val, error_expected",
    [
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.1, 0.4, 0.5], size=1000)),
            0,
            1.0,
            False,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.2, 0.3, 0.5], size=1000)),
            114.89620253164557,
            1.1235867896657214e-25,
            False,
            id="distributions are different",
        ),
        pytest.param(
            pd.Series([1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2]),
            0.0,
            1,
            True,
            id="distributions do not have enough sample for each category",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2, None], p=[0.2, 0.3, 0.4, 0.1], size=1000)),
            139.96423321882253,
            4.047183768366915e-31,
            False,
            id="distributions test data have some nulls",
        ),
    ],
)
def test_categorical_comparison(test_data, expectated_stat_val, expected_p_val, error_expected):
    from soda.scientific.distribution.comparison import (
        ChiSqAlgorithm,
        NotEnoughSamplesException,
    )

    try:
        stat_val, p_val = ChiSqAlgorithm(TEST_CONFIG_CATEGORIC_1, test_data).evaluate()
        assert expectated_stat_val == pytest.approx(stat_val, abs=1e-3)
        assert expected_p_val == pytest.approx(p_val, abs=1e-3)
        assert not error_expected
    except NotEnoughSamplesException:
        assert error_expected


@pytest.mark.parametrize(
    "test_data, config",
    [
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.1, 0.4, 0.5], size=1000)),
            RefDataCfg(bins=[0, 1], weights=[0.1, 0.9], labels=None, method="categorical"),
            id="category missing in reference data",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1], p=[0.1, 0.9], size=1000)),
            RefDataCfg(bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, method="categorical"),
            id="category missing in test data",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([None, None, 1], p=[0.2, 0.3, 0.5], size=1000)),
            RefDataCfg(bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, method="categorical"),
            id="one of the distributions is fully none",
        ),
    ],
)
def test_chi_sq_2_samples_comparison_missing_cat(test_data, config):
    from soda.scientific.distribution.comparison import (
        ChiSqAlgorithm,
        MissingCategories,
    )

    checker = ChiSqAlgorithm(config, test_data)
    with pytest.raises(MissingCategories):
        checker.evaluate()


@pytest.mark.parametrize(
    "test_data, config",
    [
        pytest.param(
            pd.Series([None, None] * 10),
            RefDataCfg(bins=[0, 1], weights=[0.1, 0.9], labels=None, method="categorical"),
            id="test data is all none",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1], p=[0.1, 0.9], size=1000)),
            RefDataCfg(bins=[None], weights=[1], labels=None, method="categorical"),
            id="ref data is all none",
        ),
        pytest.param(
            pd.Series([None, None] * 10),
            RefDataCfg(bins=[None], weights=[1], labels=None, method="categorical"),
            id="both distributions are null",
        ),
    ],
)
def test_chi_sq_2_samples_comparison_one_or_more_null_distros(test_data, config):
    from soda.scientific.distribution.comparison import ChiSqAlgorithm

    checker = ChiSqAlgorithm(config, test_data)
    with pytest.raises(AssertionError):
        checker.evaluate()


@pytest.mark.parametrize(
    "test_data, config",
    [
        pytest.param(
            pd.Series(default_rng(61).choice([1, 2], p=[0.5, 0.5], size=2)),
            RefDataCfg(bins=[1, 2], weights=[0.5, 0.5], labels=None, method="categorical"),
            id="not enough samples",
        ),
    ],
)
def test_chi_sq_2_samples_comparison_not_enough_samples(test_data, config):
    from soda.scientific.distribution.comparison import (
        ChiSqAlgorithm,
        NotEnoughSamplesException,
    )

    checker = ChiSqAlgorithm(config, test_data)
    with pytest.raises(NotEnoughSamplesException):
        checker.evaluate()
