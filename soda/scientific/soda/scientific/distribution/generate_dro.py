import logging
from typing import List

import numpy as np
import pandas as pd
from pydantic import BaseModel

from soda.scientific.distribution.utils import RefDataCfg


class DRO(BaseModel):
    weights: List
    bins: List

    class Config:
        arbitrary_types_allowed = True


def normalize(data: np.ndarray):
    return data / np.sum(data)


class DROGenerator:
    def __init__(self, cfg: RefDataCfg, data: list) -> None:
        self.method = cfg.method
        self.data = data

    def generate_continuous_dro(self) -> DRO:
        data = np.array(self.data, dtype=float)
        if (np.isnan(data)).any():
            data_len = data.shape[0]
            none_count = np.count_nonzero(np.isnan(data))
            data = data[~np.isnan(data)]
            logging.warning(
                f"""{none_count} out of {data_len} rows ({none_count/data_len}%)
                has None values! To estimate the weights and bins, the null values
                has been ignored!
            """
            )
        weights, bins = np.histogram(data, bins="auto", density=False)

        # Prepend 0 since weights and bins do not have the same size
        weights = np.insert(weights, 0, 0)
        weights = normalize(weights)
        return DRO(weights=weights.tolist(), bins=bins.tolist())

    def generate_categorical_dro(self) -> DRO:
        data = pd.Series(self.data)
        value_counts = data.value_counts()

        labels = value_counts.index.to_numpy()
        weights = value_counts.to_numpy()
        weights = normalize(weights)
        return DRO(weights=weights.tolist(), bins=labels.tolist())

    def generate(self) -> DRO:
        distribution_type_mapping = {
            "continuous": "continuous",
            "ks": "continuous",
            "categorical": "categorical",
            "chi_square": "categorical",
        }
        if distribution_type_mapping[self.method] == "continuous":
            return self.generate_continuous_dro()
        else:
            return self.generate_categorical_dro()
