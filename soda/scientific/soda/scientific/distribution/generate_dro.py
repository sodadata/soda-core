import logging
import math
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


def normalize(data: np.ndarray) -> np.ndarray:
    return data / np.sum(data)


class DROGenerator:
    def __init__(self, cfg: RefDataCfg, data: list) -> None:
        self.distribution_type = cfg.distribution_type
        self.data = data
        self.maximum_allowed_bin_size = 1e6

    @staticmethod
    def _compute_n_bins(data: np.ndarray) -> int:
        _range = (data.min(), data.max())
        bin_width = np.lib.histograms._hist_bin_auto(data, _range)
        first_edge, last_edge = np.lib.histograms._get_outer_edges(data, _range)
        n_bins = int(np.ceil(np.lib.histograms._unsigned_subtract(last_edge, first_edge) / bin_width))
        return n_bins

    @staticmethod
    def _remove_outliers_with_iqr(data: np.ndarray) -> np.ndarray:
        # Remove outliers
        q1, q3 = np.percentile(data, [25, 75])
        IQR = q3 - q1
        lower_range = q1 - (1.5 * IQR)
        upper_range = q3 + (1.5 * IQR)
        filtered_data = data[np.where((data >= lower_range) & (data <= upper_range))]
        logging.warning(
            f"""While generating the distribution reference object, the automatic bin size detection
exceeded the memory. It is generally caused by outliers in the data. You can reproduce the issue
with the following example

```
training_input = np.random.rand(7000)
training_input[1000] = 1000000000000000
np.histogram(training_input, bins="auto")
```

To fix the issue, we filtered the outliers using the Interquanlite Range (https://en.wikipedia.org/wiki/Interquartile_range)
The values  which are less than 1st Ouartile - 1.5 IQR and more than 3rd Quartile + 1.5 IQR has been removed from the data.

lower limit (1st Ouartile - 1.5 IQR) = {lower_range}
upper limit (3rd Ouartile + 1.5 IQR) = {upper_range}
"""
        )
        return filtered_data

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

        n_bins = self._compute_n_bins(data)

        # First check whether we have n_bins having lower than data size
        if n_bins < min(self.maximum_allowed_bin_size, data.size):
            weights, bins = np.histogram(data, bins="auto", density=False)

        # If we have too much n_bins, then we apply several methods
        else:
            # First remove the outliers from data and recompute number of bins
            outlier_filtered_data = self._remove_outliers_with_iqr(data)
            n_bins = self._compute_n_bins(outlier_filtered_data)

            # If n_bins is lower than data size then run auto mode again
            if n_bins < min(self.maximum_allowed_bin_size, data.size):
                weights, bins = np.histogram(outlier_filtered_data, bins="auto", density=False)

            # If not then take the sqrt of data size and make it as our new n_bins
            else:
                n_sqrt_bins = int(np.ceil(math.sqrt(outlier_filtered_data.size)))
                if n_sqrt_bins < self.maximum_allowed_bin_size:
                    logging.warning(
                        f"""Filtering out outliers does not solved the memory error, so we will
take the square root of the data size to set the number of bins where;

n_bins = {n_bins}
sqrt(n_bins) = {n_sqrt_bins}
"""
                    )
                    weights, bins = np.histogram(outlier_filtered_data, bins=n_sqrt_bins, density=False)
                else:
                    logging.warning(
                        f"""We set n_bins={self.maximum_allowed_bin_size} as maximum since your
n_bins={n_bins} which is higher then {self.maximum_allowed_bin_size}"""
                    )
                    weights, bins = np.histogram(
                        outlier_filtered_data, bins=self.maximum_allowed_bin_size, density=False
                    )

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
        if self.distribution_type == "continuous":
            return self.generate_continuous_dro()
        else:
            return self.generate_categorical_dro()
