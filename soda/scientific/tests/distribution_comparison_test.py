import decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from numpy.random import default_rng
from pydantic import ValidationError
from ruamel.yaml import YAML

from soda.scientific.distribution.comparison import (
    DistributionRefKeyException,
    DistributionRefParsingException,
    MissingCategories,
)
from soda.scientific.distribution.utils import RefDataCfg


def read_dro(dro_path: str) -> YAML:
    assets_path = Path(__file__).parent / "assets"
    f_name = dro_path.split("/")[-1]
    processed_dro_path = assets_path / f_name
    with open(processed_dro_path) as f:
        dro_yaml = f.read()
    return YAML().load(dro_yaml)


@pytest.mark.parametrize(
    "distribution_type, error_expected",
    [
        pytest.param("continuous", False, id="valid distribution_type continuous"),
        pytest.param("categorical", False, id="valid distribution_type categorical"),
        pytest.param("heyyo", True, id="invalid distribution_type heyyo"),
    ],
)
def test_config_distribution_type(distribution_type, error_expected):
    try:
        bins = [1, 2, 3]
        weights = [0.1, 0.8, 0.1]
        RefDataCfg(bins=bins, weights=weights, labels=None, distribution_type=distribution_type)
    except ValidationError:
        assert error_expected


@pytest.mark.parametrize(
    "weights, error_expected",
    [
        pytest.param([0.5, 0.3, 0.2], False, id="valid weights with sum == 1"),
        pytest.param([0.5, 0.5, 0.5], True, id="invalid weights with sum != 1"),
        pytest.param([None, 0.5, 0.5], True, id="invalid weights with sum == 1 but having none"),
    ],
)
def test_config_weights(weights, error_expected):
    bins = [1, 2, 3]
    try:
        RefDataCfg(bins=bins, weights=weights, labels=None, distribution_type="continuous")
    except ValidationError:
        assert error_expected


@pytest.mark.parametrize(
    "method, reference_file_path, test_data, expected_stat, expected_p",
    [
        pytest.param(
            "ks",
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            list(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.036,
            0.5545835690881001,
            id="Similar continuous distribution with ks",
        ),
        pytest.param(
            "ks",
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            list(default_rng(61).normal(loc=1.5, scale=1.0, size=1000)),
            0.2115,
            8.845435165401255e-20,
            id="Different continuous distribution with ks",
        ),
        pytest.param(
            "chi_square",
            "soda/scientific/tests/assets/dist_ref_categorical.yml",
            [(1, 700), (2, 200), (3, 100)],
            1.4244611103249847,
            0.4905487801068025,
            id="Different categorical distribution with chi-square",
        ),
        pytest.param(
            "psi",
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            [1.0, decimal.Decimal("2.5"), decimal.Decimal("0.1"), decimal.Decimal("-2.5")],
            None,
            10.126226038606184,
            id="Different continuous distribution with psi and decimal values",
        ),
        pytest.param(
            "swd",
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            [1.0, 2.5, decimal.Decimal("0.1")],
            None,
            0.7177783796435317,
            id="Different continuous distribution with swd and decimal values",
        ),
        pytest.param(
            "semd",
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            [1.0, 2.5, decimal.Decimal("0.1")],
            None,
            0.7177783796435317,
            id="Different continuous distribution with semd and decimal values",
        ),
    ],
)
def test_distribution_checker(method, reference_file_path, test_data, expected_stat, expected_p):
    from soda.scientific.distribution.comparison import DistributionChecker

    parsed_dro = read_dro(reference_file_path)
    check = DistributionChecker(
        dist_method=method,
        parsed_dro=parsed_dro,
        dist_ref_file_path=reference_file_path,
        dist_name=None,
        data=test_data,
    )
    check_results = check.run()
    assert check_results["stat_value"] == pytest.approx(expected_stat, abs=1e-3)
    assert check_results["check_value"] == pytest.approx(expected_p, abs=1e-3)


@pytest.mark.parametrize(
    "reference_file_path, exception",
    [
        pytest.param(
            "soda/scientific/tests/assets/dist_ref_missing_method.yml",
            DistributionRefKeyException,
            id="Missing key method",
        )
    ],
)
def test_ref_config_file_exceptions(reference_file_path, exception):
    from soda.scientific.distribution.comparison import DistributionChecker

    with pytest.raises(exception):
        test_data = list(pd.Series(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)))
        parsed_dro = read_dro(reference_file_path)
        DistributionChecker(
            dist_method="continuous",
            parsed_dro=parsed_dro,
            dist_ref_file_path=reference_file_path,
            dist_name=None,
            data=test_data,
        )


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
    distribution_type="continuous",
)


@pytest.mark.parametrize(
    "test_data, expected_stat_val, expected_p_val",
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
def test_ks_comparison(test_data, expected_stat_val, expected_p_val):
    from soda.scientific.distribution.comparison import KSAlgorithm

    check_results = KSAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert expected_stat_val == pytest.approx(check_results["stat_value"], abs=1e-3)
    assert expected_p_val == pytest.approx(check_results["check_value"], abs=1e-3)


# The following bins and weights are generated based on
# default_rng().normal(loc=1.0, scale=1.0, size=1000)
TEST_CONFIG_CATEGORIC_1 = RefDataCfg(
    bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, distribution_type="categorical"
)


@pytest.mark.parametrize(
    "test_data, expected_stat_val, expected_p_val",
    [
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 500)]),
            0,
            1.0,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series([(0, 200), (1, 300), (2, 500)]),
            123.94755685044814,
            1.2165501237202362e-27,
            id="distributions are different",
        ),
    ],
)
def test_chi_square_comparison(test_data, expected_stat_val, expected_p_val):
    from soda.scientific.distribution.comparison import ChiSqAlgorithm

    check_results = ChiSqAlgorithm(TEST_CONFIG_CATEGORIC_1, test_data).evaluate()
    assert expected_stat_val == pytest.approx(check_results["stat_value"], abs=1e-3)
    assert expected_p_val == pytest.approx(check_results["check_value"], abs=1e-3)


@pytest.mark.parametrize(
    "test_data, config",
    [
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 500)]),
            RefDataCfg(bins=[0, 1], weights=[0.1, 0.9], labels=None, distribution_type="categorical"),
            id="category missing in reference data",
        ),
        pytest.param(
            pd.Series([(0, 100), (1, 900)]),
            RefDataCfg(bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, distribution_type="categorical"),
            id="category missing in test data",
        ),
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 400), (None, 100)]),
            RefDataCfg(bins=[0, 1, 2], weights=[0.1, 0.4, 0.5], labels=None, distribution_type="categorical"),
            id="one of the distribution has nulls",
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
            pd.Series([(None, 20)]),
            RefDataCfg(bins=[0, 1], weights=[0.1, 0.9], labels=None, distribution_type="categorical"),
            id="test data is all none",
        ),
        pytest.param(
            pd.Series([(0, 100), (1, 900)]),
            RefDataCfg(bins=[None], weights=[1], labels=None, distribution_type="categorical"),
            id="ref data is all none",
        ),
        pytest.param(
            pd.Series([(None, 20)]),
            RefDataCfg(bins=[None], weights=[1], labels=None, distribution_type="categorical"),
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
            pd.Series(
                [
                    (1, 1),
                    (2, 1),
                ]
            ),
            RefDataCfg(bins=[1, 2], weights=[0.5, 0.5], labels=None, distribution_type="categorical"),
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


@pytest.mark.parametrize(
    "test_data, expected_swd",
    [
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.09541381487225127,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=0.9, size=1000)),
            0.10607140143389837,
            id="distributions are different_1",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=0.9, scale=1.0, size=1000)),
            0.19193533134279744,
            id="distributions are different_2",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=10, scale=10, size=1000)),
            1.2438664038653537,
            id="distributions are extremely different",
        ),
    ],
)
def test_swd_continuous(test_data, expected_swd):
    from soda.scientific.distribution.comparison import SWDAlgorithm

    check_results = SWDAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert check_results["check_value"] == expected_swd


@pytest.mark.parametrize(
    "test_data, expected_swd",
    [
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 500)]),
            0,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series([(0, 200), (1, 300), (2, 500)]),
            0.4472135954999579,
            id="distributions are different",
        ),
    ],
)
def test_swd_categorical(test_data, expected_swd):
    from soda.scientific.distribution.comparison import SWDAlgorithm

    check_results = SWDAlgorithm(TEST_CONFIG_CATEGORIC_1, test_data).evaluate()
    assert check_results["check_value"] == expected_swd


@pytest.mark.parametrize("test_data", [pd.Series(100 * [np.nan])])
def test_swd_comparison_null(test_data):
    from soda.scientific.distribution.comparison import SWDAlgorithm

    check_results = SWDAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert np.isnan(check_results["check_value"])


@pytest.mark.parametrize(
    "test_data, expected_psi",
    [
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=1.0, size=1000)),
            0.040699470804929805,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=1.0, scale=0.9, size=1000)),
            0.08448405985073384,
            id="distributions are different_1",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=0.9, scale=1.0, size=1000)),
            0.06596002459912605,
            id="distributions are different_2",
        ),
        pytest.param(
            pd.Series(default_rng(61).normal(loc=10, scale=10, size=1000)),
            8.478605750455749,
            id="distributions are extremely different",
        ),
    ],
)
def test_psi_continuous(test_data, expected_psi):
    from soda.scientific.distribution.comparison import PSIAlgorithm

    check_results = PSIAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert check_results["check_value"] == expected_psi


@pytest.mark.parametrize(
    "test_data, expected_psi",
    [
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 500)]),
            0,
            id="distributions are same",
        ),
        pytest.param(
            pd.Series([(0, 200), (1, 300), (2, 500)]),
            13.885334329209103,
            id="distributions are different",
        ),
    ],
)
def test_psi_categorical(test_data, expected_psi):
    from soda.scientific.distribution.comparison import PSIAlgorithm

    check_results = PSIAlgorithm(TEST_CONFIG_CATEGORIC_1, test_data).evaluate()
    assert check_results["check_value"] == expected_psi


@pytest.mark.parametrize("test_data, expected_psi", [(pd.Series(100 * [np.nan]), 11.51281033571558)])
def test_psi_comparison_null(test_data, expected_psi):
    from soda.scientific.distribution.comparison import PSIAlgorithm

    check_results = PSIAlgorithm(TEST_CONFIG_CONT_1, test_data).evaluate()
    assert check_results["check_value"] == expected_psi


@pytest.mark.parametrize(
    "test_data, dist_ref_file_path, method",
    [
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.1, 0.4, 0.5], size=1000)),
            "soda/scientific/tests/assets/dist_ref_categorical.yml",
            "ks",
            id="ks method with distribution_type categorical",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.1, 0.4, 0.5], size=1000)),
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            "chi_square",
            id="chi_square method with distribution_type continuous",
        ),
    ],
)
def test_ref_config_incompatible(test_data, dist_ref_file_path, method):
    from soda.scientific.distribution.comparison import (
        DistributionChecker,
        DistributionRefIncompatibleException,
    )

    with pytest.raises(DistributionRefIncompatibleException):
        parsed_dro = read_dro(dist_ref_file_path)
        DistributionChecker(
            dist_method=method,
            parsed_dro=parsed_dro,
            dist_ref_file_path=dist_ref_file_path,
            dist_name=None,
            data=test_data,
        )


@pytest.mark.parametrize(
    "test_data, dist_ref_file_path, method",
    [
        pytest.param(
            pd.Series([(0, 100), (1, 400), (2, 500)]),
            "soda/scientific/tests/assets/dist_ref_categorical_no_bins.yml",
            "chi_square",
            id="missing bins and weights with with distribution_type categorical",
        ),
        pytest.param(
            pd.Series(default_rng(61).choice([0, 1, 2], p=[0.1, 0.4, 0.5], size=1000)),
            "soda/scientific/tests/assets/dist_ref_continuous_no_bins.yml",
            "ks",
            id="missing bins and weights with distribution_type continuous",
        ),
    ],
)
def test_missing_bins_weights(test_data, dist_ref_file_path, method):
    from soda.scientific.distribution.comparison import (
        DistributionChecker,
        MissingBinsWeightsException,
    )

    with pytest.raises(MissingBinsWeightsException):
        parsed_dro = read_dro(dist_ref_file_path)
        DistributionChecker(
            dist_method=method,
            parsed_dro=parsed_dro,
            dist_ref_file_path=dist_ref_file_path,
            dist_name=None,
            data=test_data,
        )


@pytest.mark.parametrize(
    "test_data, dist_ref_file_path, method",
    [
        pytest.param(
            pd.Series([]),
            "soda/scientific/tests/assets/dist_ref_continuous.yml",
            "ks",
            id="empty test data",
        ),
    ],
)
def test_empty_test_data(test_data, dist_ref_file_path, method):
    from soda.scientific.distribution.comparison import (
        DistributionChecker,
        EmptyDistributionCheckColumn,
    )

    with pytest.raises(EmptyDistributionCheckColumn):
        parsed_dro = read_dro(dist_ref_file_path)
        DistributionChecker(
            dist_method=method,
            parsed_dro=parsed_dro,
            dist_ref_file_path=dist_ref_file_path,
            dist_name=None,
            data=test_data,
        )
