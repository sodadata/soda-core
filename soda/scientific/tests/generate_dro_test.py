import numpy as np
import pytest
from numpy.random import default_rng
from numpy.testing import assert_almost_equal

from soda.scientific.distribution.utils import RefDataCfg

rng = default_rng(1234)


@pytest.mark.parametrize(
    "cfg, data, expected_weights, expected_bins",
    [
        pytest.param(
            RefDataCfg(distribution_type="continuous"),
            list(rng.normal(loc=2, scale=1.0, size=1000)),
            np.array(
                [0, 2, 4, 12, 15, 40, 49, 57, 58, 87, 112, 112, 98, 88, 74, 67, 40, 29, 27, 13, 6, 1, 7, 0, 0, 1, 1]
            ),
            np.array(
                [
                    -0.76828668,
                    -0.50476956,
                    -0.24125244,
                    0.02226468,
                    0.2857818,
                    0.54929893,
                    0.81281605,
                    1.07633317,
                    1.33985029,
                    1.60336741,
                    1.86688453,
                    2.13040166,
                    2.39391878,
                    2.6574359,
                    2.92095302,
                    3.18447014,
                    3.44798726,
                    3.71150439,
                    3.97502151,
                    4.23853863,
                    4.50205575,
                    4.76557287,
                    5.02909,
                    5.29260712,
                    5.55612424,
                    5.81964136,
                    6.08315848,
                ]
            ),
            id="continuous data",
        ),
    ],
)
def test_generate_dro_continuous(cfg, data, expected_bins, expected_weights):
    from soda.scientific.distribution.generate_dro import DROGenerator

    dro_generator = DROGenerator(cfg, data)
    dro = dro_generator.generate()

    assert_almost_equal(dro.weights, expected_weights / np.sum(expected_weights))
    assert_almost_equal(dro.bins, expected_bins)


@pytest.mark.parametrize(
    "cfg, data, expected_weights, expected_bins",
    [
        pytest.param(
            RefDataCfg(distribution_type="categorical"),
            list(rng.choice(["hello", "world", "foo"], p=[0.1, 0.4, 0.5], size=1000)),  # type: ignore
            np.array([0.525, 0.392, 0.083]),
            np.array(["foo", "world", "hello"]),
            id="categorical data",
        ),
    ],
)
def test_generate_dro_categorical(cfg, data, expected_weights, expected_bins):
    from soda.scientific.distribution.generate_dro import DROGenerator

    dro_generator = DROGenerator(cfg, data)
    dro = dro_generator.generate()

    assert_almost_equal(dro.weights, expected_weights)
    assert dro.bins == expected_bins.tolist()
