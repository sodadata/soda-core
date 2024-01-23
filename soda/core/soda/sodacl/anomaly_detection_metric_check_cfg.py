from __future__ import annotations

from typing import Any, ClassVar, List, Optional, Union

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator
from soda.common.logs import Logs
from soda.sodacl.change_over_time_cfg import ChangeOverTimeCfg
from soda.sodacl.location import Location
from soda.sodacl.metric_check_cfg import MetricCheckCfg
from soda.sodacl.missing_and_valid_cfg import MissingAndValidCfg
from soda.sodacl.threshold_cfg import ThresholdCfg


class ADBaseModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="forbid")
    logger: ClassVar[Logs]
    location: ClassVar[Location]

    @classmethod
    def create_instance(cls, logger: Logs, location: Location, **kwargs: Any) -> ADBaseModel | None:
        try:
            return cls(**kwargs)
        except ValidationError as e:
            for error in e.errors():
                # only keep string instance field names
                field_names = [loc for loc in error["loc"] if isinstance(loc, str)]
                field_name = field_names[-1]
                expected_field_dtype_str = error.get("type")
                field_value = error.get("input")  # Get the provided value
                if error.get("type") == "missing":
                    logger.error(
                        f"Anomaly Detection Parsing Error: Missing field '{field_name}' at {location}."
                        f" Configure the required field in your SodaCL file."
                    )
                elif error.get("type") == "extra_forbidden":
                    logger.error(
                        f"Anomaly Detection Parsing Error: Extra field '{field_name}' at {location}."
                        f" Remove the field from your SodaCL file."
                    )
                else:
                    logger.error(
                        "Anomaly Detection Parsing Error: Data type mismatch "
                        f"for field '{field_name}' at {location}. "
                        f"Expected type: '{expected_field_dtype_str}', "
                        f"but found value '{field_value}' of type '{type(field_value).__name__}'."
                    )
            return None
        except ValueError as e:
            logger.error(f"Error while parsing {cls.__name__} at {location}:\n{e}")
            return None


class ProphetDefaultHyperparameters(ADBaseModel):
    growth: str = "linear"
    changepoints: Any = None
    n_changepoints: int = 25
    changepoint_range: float = 0.8
    yearly_seasonality: Any = "auto"
    weekly_seasonality: Any = "auto"
    daily_seasonality: Any = "auto"
    holidays: Any = None
    seasonality_mode: str = "multiplicative"  # Tuned
    seasonality_prior_scale: float = 0.01  # Tuned
    holidays_prior_scale: float = 10.0
    changepoint_prior_scale: float = 0.001  # Tuned
    mcmc_samples: int = 0
    interval_width: float = 0.999  # Tuned
    uncertainty_samples: int = 1000
    stan_backend: Any = None
    scaling: str = "absmax"
    holidays_mode: Any = None


class ProphetMAPEProfileHyperparameters(ProphetDefaultHyperparameters):
    seasonality_prior_scale: float = 0.1  # Tuned
    changepoint_prior_scale: float = 0.1  # Tuned


class ProphetParameterGrid(ADBaseModel):
    growth: List[str] = ["linear"]
    changepoints: List[Any] = [None]
    n_changepoints: List[int] = [25]
    changepoint_range: List[float] = [0.8]
    yearly_seasonality: List[Any] = ["auto"]
    weekly_seasonality: List[Any] = ["auto"]
    daily_seasonality: List[Any] = ["auto"]
    holidays: List[Any] = [None]
    seasonality_mode: List[str] = ["multiplicative"]  # Non default
    seasonality_prior_scale: List[float] = [0.01, 0.1, 1.0, 10.0]  # Non default
    holidays_prior_scale: List[float] = [10.0]
    changepoint_prior_scale: List[float] = [0.001, 0.01, 0.1, 0.5]  # Non default
    mcmc_samples: List[int] = [0]
    interval_width: List[float] = [0.999]  # Non default
    stan_backend: List[Any] = [None]
    scaling: List[str] = ["absmax"]
    holidays_mode: List[Any] = [None]


class ProphetDynamicHyperparameters(ADBaseModel):
    objective_metric: Union[str, List[str]]
    parallelize_cross_validation: bool = True
    cross_validation_folds: int = 5
    frequency: int = 10
    parameter_grid: ProphetParameterGrid = ProphetParameterGrid()

    @field_validator("objective_metric", mode="before")
    @classmethod
    def metric_is_allowed(cls, v: str | List[str]) -> str | List[str]:
        allowed_metrics = ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]
        error_message = (
            "objective_metric: '{objective_metric}' is not allowed. "
            "Please choose from 'mse', 'rmse', 'mae', 'mape', 'mdape', 'smape', 'coverage'."
        )
        if isinstance(v, List):
            v = [metric.lower() for metric in v]
            for metric in v:
                if metric not in allowed_metrics:
                    raise ValueError(error_message.format(objective_metric=metric))
        else:
            if v.lower() not in ["mse", "rmse", "mae", "mape", "mdape", "smape", "coverage"]:
                raise ValueError(error_message.format(objective_metric=v))
        return v


class ProphetCustomHyperparameters(ADBaseModel):
    custom_hyperparameters: ProphetDefaultHyperparameters = ProphetDefaultHyperparameters()


class ProphetHyperparameterProfiles(ADBaseModel):
    profile: ProphetCustomHyperparameters = ProphetCustomHyperparameters()

    @field_validator("profile", mode="before")
    def set_profile(cls, v: Union[str, ProphetCustomHyperparameters]) -> ProphetCustomHyperparameters:
        if isinstance(v, str):
            v = v.lower()
            if v == "mape":
                return ProphetCustomHyperparameters(custom_hyperparameters=ProphetMAPEProfileHyperparameters())
            elif v == "coverage":
                return ProphetCustomHyperparameters()
            else:
                raise ValueError(f"Profile: '{v}' is not allowed. " f"Please choose from 'MAPE' or 'coverage'.")
        else:
            return v


class HyperparameterConfigs(ADBaseModel):
    static: ProphetHyperparameterProfiles = ProphetHyperparameterProfiles()
    dynamic: Optional[ProphetDynamicHyperparameters] = None


class ModelConfigs(ADBaseModel):
    type: str = "prophet"
    hyperparameters: HyperparameterConfigs = HyperparameterConfigs()


class TrainingDatasetParameters(ADBaseModel):
    frequency: str = "auto"
    aggregation_function: str = "last"
    window_length: int = 1000


class AnomalyDetectionMetricCheckCfg(MetricCheckCfg):
    def __init__(
        self,
        source_header: str,
        source_line: str,
        source_configurations: str | None,
        location: Location,
        name: str | None,
        metric_name: str,
        metric_args: List[object] | None,
        missing_and_valid_cfg: MissingAndValidCfg | None,
        filter: str | None,
        condition: str | None,
        metric_expression: str | None,
        metric_query: str | None,
        change_over_time_cfg: ChangeOverTimeCfg | None,
        fail_threshold_cfg: ThresholdCfg | None,
        warn_threshold_cfg: ThresholdCfg | None,
        model_cfg: ModelConfigs,
        training_dataset_params: TrainingDatasetParameters,
        is_automated_monitoring: bool = False,
        samples_limit: int | None = None,
        samples_columns: List | None = None,
    ):
        super().__init__(
            source_header,
            source_line,
            source_configurations,
            location,
            name,
            metric_name,
            metric_args,
            missing_and_valid_cfg,
            filter,
            condition,
            metric_expression,
            metric_query,
            change_over_time_cfg,
            fail_threshold_cfg,
            warn_threshold_cfg,
            samples_limit=samples_limit,
        )
        self.is_automated_monitoring = is_automated_monitoring
        self.model_cfg = model_cfg
        self.training_dataset_params = training_dataset_params
