from __future__ import annotations

import re
from ast import literal_eval
from collections import OrderedDict
from logging import Logger
from pathlib import Path

import streamlit as st
from soda.common.logs import Logs
from soda.sodacl.anomaly_detection_metric_check_cfg import (
    HyperparameterConfigs,
    ModelConfigs,
    ProphetCustomHyperparameters,
    ProphetDefaultHyperparameters,
    ProphetHyperparameterProfiles,
    SeverityLevelParameters,
    TrainingDatasetParameters,
)

from soda.scientific.anomaly_detection_v2.anomaly_detector import AnomalyDetector
from soda.scientific.anomaly_detection_v2.exceptions import (
    AuthenticationException,
    CheckIDNotFoundException,
)
from soda.scientific.anomaly_detection_v2.simulate.anomaly_detection_dataset import (
    AnomalyDetectionData,
)
from soda.scientific.anomaly_detection_v2.simulate.globals import (
    HYPERPARAMETER_PROFILE_DOC,
)
from soda.scientific.anomaly_detection_v2.simulate.pydantic_models import (
    AnomalyDetectionResults,
    EvaluateOutput,
)
from soda.scientific.anomaly_detection_v2.simulate.visualisation import (
    visualize_results,
)

FILE_PATH = Path(__file__).parent.absolute()
ASSETS_PATH = FILE_PATH / "assets"


def get_severity_level_params_cfg() -> SeverityLevelParameters:
    st.sidebar.subheader("Severity Level Parameters")
    warning_ratio = st.sidebar.slider(
        label="warning_ratio",
        min_value=0.0,
        max_value=1.0,
        value=0.1,
        step=0.01,
    )
    min_confidence_interval_ratio = st.sidebar.slider(
        label="min_confidence_interval_ratio",
        min_value=0.0,
        max_value=0.1,
        value=0.001,
        step=0.001,
        format="%.3f",
    )
    severity_level_parameters = SeverityLevelParameters(
        warning_ratio=warning_ratio,
        min_confidence_interval_ratio=min_confidence_interval_ratio,
    )
    return severity_level_parameters


def get_model_cfg() -> ModelConfigs:
    st.sidebar.subheader("Model Hyperparameter Profiles")
    profile = st.sidebar.radio(
        "Choose hyperparameter profile:",
        ("coverage", "MAPE", "custom"),
        index=0,
        help=f"Refer to {HYPERPARAMETER_PROFILE_DOC} for more information about hyperparameter profiles",
    )

    if profile == "custom":
        st.sidebar.subheader("Custom Prophet Hyperparameters")
        is_advanced = st.sidebar.toggle("Advanced", value=False)
        if is_advanced:
            profile = "coverage"  # set profile to coverage to avoid error
            custom_parameters_json = st.sidebar.text_area(
                label="Custom Prophet Parameters",
                placeholder=(
                    "Paste your custom prophet hyperparameters here in JSON format"
                    "\n\nExample:\n"
                    '{"changepoint_prior_scale": 0.001, "seasonality_prior_scale": 0.01, "seasonality_mode": "multiplicative"}'
                ),
                height=300,
                max_chars=1000,
            )
            if custom_parameters_json:
                try:
                    # Parse JSON to dict safely
                    custom_parameters_dict = literal_eval(custom_parameters_json)
                    profile = ProphetCustomHyperparameters(
                        custom_hyperparameters=ProphetDefaultHyperparameters(**custom_parameters_dict)
                    )
                    st.sidebar.success("Custom hyperparameters loaded successfully")
                except:
                    profile = "coverage"  # set profile to coverage to avoid error
                    st.sidebar.error(
                        "Invalid JSON format"
                        "\nExample:\n"
                        '{"changepoint_prior_scale": 0.001, "seasonality_prior_scale": 0.01, "seasonality_mode": "multiplicative"}'
                    )
        else:
            profile = ProphetCustomHyperparameters(
                custom_hyperparameters=ProphetDefaultHyperparameters(
                    changepoint_prior_scale=st.sidebar.slider(
                        label="changepoint_prior_scale",
                        min_value=0.001,
                        max_value=0.5,
                        value=0.001,
                        step=0.001,
                        format="%f",
                    ),
                    seasonality_prior_scale=st.sidebar.slider(
                        label="seasonality_prior_scale",
                        min_value=0.01,
                        max_value=10.0,
                        value=0.01,
                        step=0.01,
                        format="%f",
                    ),
                    seasonality_mode=st.sidebar.selectbox(
                        "seasonality_mode", ("multiplicative", "additive")
                    ),  # type: ignore
                )
            )
    model_cfg = ModelConfigs(
        hyperparameters=HyperparameterConfigs(
            static=ProphetHyperparameterProfiles(
                profile=profile,  # type: ignore
            ),
        )
    )
    return model_cfg


def get_training_dataset_params_cfg() -> TrainingDatasetParameters:
    st.sidebar.subheader("Training Dataset Parameters")
    window_length = st.sidebar.slider(
        label="window_length",
        min_value=5,
        max_value=1000,
        value=1000,
        step=1,
    )

    frequency_mapping = OrderedDict(
        {
            "auto": "auto",
            "T (minute)": "T",
            "H (hour)": "H",
            "D (day)": "D",
            "W (week)": "W",
            "M (month end)": "M",
            "MS (month start)": "MS",
            "Q (quarter end)": "Q",
            "QS (quarter start)": "QS",
            "A (year end)": "A",
            "AS (year start)": "AS",
        }
    )

    frequency = st.sidebar.selectbox(
        label="frequency",
        options=list(frequency_mapping.keys()),
        index=0,
    )
    frequency = frequency_mapping[frequency]
    aggregation_function = st.sidebar.selectbox(
        label="aggregation_function",
        options=["last", "first", "min", "max", "mean", "median"],
        index=0,
    )
    training_dataset_params_cfg = TrainingDatasetParameters(
        window_length=window_length,
        frequency=frequency,
        aggregation_function=aggregation_function,
    )
    return training_dataset_params_cfg


def simulate(
    anomaly_detection_data: AnomalyDetectionData,
    model_cfg: ModelConfigs,
    training_dataset_params_cfg: TrainingDatasetParameters,
    severity_level_params_cfg: SeverityLevelParameters,
    n_last_records_to_simulate: int,
) -> AnomalyDetectionResults:
    results = []
    all_measurements = anomaly_detection_data.measurements["results"]
    all_check_results = anomaly_detection_data.check_results["results"]
    assert len(all_measurements) == len(all_check_results), (
        f"number of measurements must be equal to number of check results. "
        f"Got {len(all_measurements['results'])} measurements and {len(all_check_results['results'])} check results"
    )
    n_records = len(all_measurements)
    starting_point = n_records - n_last_records_to_simulate

    # Create a progress bar
    progress_bar = st.progress(0, "Simulating anomaly detection, please wait...")
    progress_counter = 0

    for i in range(starting_point, n_records):
        temp_measurements = all_measurements[: i + 1]
        temp_check_results = all_check_results[: i + 1]
        detector = AnomalyDetector(
            measurements={"results": temp_measurements},
            check_results={"results": temp_check_results},
            logs=Logs(Logger("soda.core")),
            model_cfg=model_cfg,
            training_dataset_params=training_dataset_params_cfg,
            severity_level_params=severity_level_params_cfg,
        )
        level, diagnostics = detector.evaluate()

        results.append(
            EvaluateOutput(
                dataTime=str(temp_measurements[-1]["dataTime"]),
                level=level,
                **diagnostics,
            )
        )
        progress_counter += 1
        progress_percent = int(progress_counter / (n_records - starting_point) * 100)
        progress_bar.progress(progress_percent)
    st.success("Task completed!")
    results = AnomalyDetectionResults.model_validate({"results": results})
    return results


@st.cache_data
def get_anomaly_detection_data(check_id: str) -> AnomalyDetectionData | None:
    anomaly_detection_data = None
    if check_id == "":
        return anomaly_detection_data
    try:
        anomaly_detection_data = AnomalyDetectionData(check_id=check_id)
    except AuthenticationException as e:
        st.error(f"Soda Cloud Authentication Error: {e}")
    except CheckIDNotFoundException as e:
        st.error(f"Check ID Not Found Error: {e}")
    except Exception as e:
        st.error(f"Error: {e}")
    return anomaly_detection_data


def get_check_id() -> str:
    text_field_message = "Enter your check URL to start your simulation:"
    help_message = (
        "In Soda Cloud UI, browse to the checks page "
        "and click on the anomaly detection check you want to simulate. "
        "Copy the URL and paste it here."
    )

    if "check_url" not in st.session_state:
        check_url = st.text_input(text_field_message, help=help_message)
    else:
        check_url = st.text_input(text_field_message, value=st.session_state["check_url"], help=help_message)

    # From the check URL, extract the check_id
    # Find the value between "checks/" and "/"
    regex_pattern = re.compile(r"checks\/([a-zA-Z0-9-]+)\/")
    check_id_match = regex_pattern.search(check_url)

    check_id = ""
    if check_id_match:
        check_id = check_id_match.group(1)
        st.session_state["check_url"] = check_url
    elif check_id_match is None and check_url != "":
        st.error("Invalid check URL. Please enter a valid check URL. " + help_message)
    return check_id


def main() -> None:
    st.set_page_config(
        layout="wide",
        page_title="Anomaly Detection Simulator",
        page_icon=str(ASSETS_PATH / "favicon.ico"),
    )
    st.sidebar.image(str(ASSETS_PATH / "SODA.svg"))

    # Set the title of the app
    st.title("Anomaly Detection Parameter Simulator")

    check_id = get_check_id()
    model_cfg = get_model_cfg()
    training_dataset_params_cfg = get_training_dataset_params_cfg()
    severity_level_params_cfg = get_severity_level_params_cfg()

    anomaly_detection_data = get_anomaly_detection_data(check_id=check_id)

    # Ask number of last records to simulate
    if anomaly_detection_data is not None:
        st.subheader(f"Simulated check id: {check_id}")

        total_n_records = len(anomaly_detection_data.measurements["results"])
        n_last_records_to_simulate = st.slider(
            label="Simulate most recent n records (1 is the most recent)",
            min_value=1,
            max_value=total_n_records,
            value=total_n_records,
            step=1,
        )

        # Add button to start the simulation
        simulate_button = st.button("Start Simulation")
        if simulate_button:
            simulation_results = simulate(
                anomaly_detection_data=anomaly_detection_data,
                model_cfg=model_cfg,
                training_dataset_params_cfg=training_dataset_params_cfg,
                severity_level_params_cfg=severity_level_params_cfg,
                n_last_records_to_simulate=n_last_records_to_simulate,
            )
            fig = visualize_results(results=simulation_results)
            st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
