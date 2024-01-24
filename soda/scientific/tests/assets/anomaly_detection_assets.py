import pandas as pd
from numpy import nan

test_anomaly_detector_evaluate_historic_measurements = {
    "results": [
        {
            "id": "49d198f1-eda7-42ad-bd70-5e1789bdf122",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": "2022-04-20T15:05:30Z",
        },
        {
            "id": "959e5167-39e0-481b-9939-8ff7393391a5",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": "2022-04-19T15:05:10Z",
        },
        {
            "id": "efc8f472-3d74-4a9a-965f-de14dcf4b2a9",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 2.0,
            "dataTime": "2022-04-18T14:49:59Z",
        },
        {
            "id": "42a2b60b-d932-411d-9cab-bf7c33a84c65",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 1.0,
            "dataTime": "2022-04-17T14:49:20Z",
        },
        {
            "id": "3ef53638-04cc-4614-b587-a059a81a4c2f",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 1.0,
            "dataTime": "2022-04-16T14:47:44Z",
        },
        {
            "id": "b7dd6e88-f7a0-42c4-87c1-0662eb0e2ce5",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": "2022-04-15T15:04:42Z",
        },
    ]
}

test_anomaly_detector_evaluate_historic_check_results = {
    "results": [
        {
            "type": "anomalyDetection",
            "testId": "8cedb608-e8be-4147-83be-0f86972788a9",
            "testResultId": "97a8ce8b-d43d-4f5e-932b-b1103c637fe7",
            "measurementId": "49d198f1-eda7-42ad-bd70-5e1789bdf122",
            "outcome": "pass",
            "dataTime": "2022-04-20T15:05:30Z",
            "diagnostics": {
                "value": 21.0,
                "anomalyPredictedValue": 20.870516335508597,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "fail": {
                    "lessThanOrEqual": 20.15142652696,
                    "greaterThanOrEqual": 22.03561767424,
                },
                "warn": {
                    "lessThanOrEqual": 20.3084424559,
                    "greaterThanOrEqual": 21.8786017453,
                },
            },
        },
        {
            "type": "anomalyDetection",
            "testId": "8cedb608-e8be-4147-83be-0f86972788a9",
            "testResultId": "5ebf531e-83dd-445d-ac91-42cd9af45be2",
            "measurementId": "7fd4f85b-37b6-46f7-b6b8-56af91b3f920",
            "outcome": "pass",
            "dataTime": "2022-04-12T15:00:31Z",
            "diagnostics": {
                "value": 2.0,
                "anomalyPredictedValue": 8.46757918589978,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "fail": {
                    "lessThanOrEqual": -1.89282665126,
                    "greaterThanOrEqual": 17.89954537186,
                },
                "warn": {
                    "lessThanOrEqual": -0.243462316,
                    "greaterThanOrEqual": 16.2501810366,
                },
            },
        },
        {
            "type": "anomalyDetection",
            "testId": "8cedb608-e8be-4147-83be-0f86972788a9",
            "testResultId": "18b6fc89-fea3-4a5f-8c8f-bde7634d016c",
            "measurementId": "d926b795-cf98-4e96-9eea-bf688c86d773",
            "outcome": "pass",
            "dataTime": "2022-04-12T14:59:28Z",
            "diagnostics": {
                "value": 2.0,
                "anomalyPredictedValue": 8.46757918589978,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "fail": {
                    "lessThanOrEqual": -2.54660323027,
                    "greaterThanOrEqual": 18.60637946177,
                },
                "warn": {
                    "lessThanOrEqual": -0.7838546726,
                    "greaterThanOrEqual": 16.8436309041,
                },
            },
        },
    ]
}

test_anomaly_detector_evaluate_expected_results = {
    "value": 21.0,
    "fail": {"greaterThanOrEqual": 42.59848061337, "lessThanOrEqual": -10.97031870027},
    "warn": {"greaterThanOrEqual": 38.1344140039, "lessThanOrEqual": -6.5062520908},
    "anomalyPredictedValue": 15.419588986390275,
    "anomalyErrorSeverity": "pass",
    "anomalyErrorCode": "",
    "feedback": {
        "isCorrectlyClassified": "None",
        "isAnomaly": "None",
        "reason": "None",
        "freeTextReason": "None",
        "skipMeasurements": "None",
    },
}

test_prophet_model_skip_measurements_this_exclusive_previous = pd.DataFrame(
    {
        "y": {
            0: 245.0,
            1: 45.0,
            2: 40.0,
            3: 35.0,
            4: 30.0,
            5: 25.0,
            6: 20.0,
            7: 15.0,
            8: 10.0,
            9: 5.0,
            10: 250.0,
        },
        "ds": {
            0: "2023-02-15 11:00:00",
            1: "2023-02-14 11:00:00",
            2: "2023-02-13 11:00:00",
            3: "2023-02-12 11:00:00",
            4: "2023-02-11 11:00:00",
            5: "2023-02-10 11:00:00",
            6: "2023-02-09 11:00:00",
            7: "2023-02-08 11:00:00",
            8: "2023-02-07 11:00:00",
            9: "2023-02-06 11:00:00",
            10: "2023-02-16 11:00:00",
        },
        "skipMeasurements": {
            0: None,
            1: "this",
            2: None,
            3: None,
            4: None,
            5: "previous",
            6: None,
            7: None,
            8: None,
            9: None,
            10: nan,
        },
    }
)

test_prophet_model_skip_measurements_this_exclusive_previous_expectation = pd.DataFrame(
    {
        "ds": {
            0: "2023-02-10 11:00:00",
            1: "2023-02-11 11:00:00",
            2: "2023-02-12 11:00:00",
            3: "2023-02-13 11:00:00",
            4: "2023-02-14 11:00:00",
            5: "2023-02-15 11:00:00",
            6: "2023-02-16 11:00:00",
        },
        "y": {0: 25.0, 1: 30.0, 2: 35.0, 3: 40.0, 4: nan, 5: 245.0, 6: 250.0},
    }
)


test_prophet_model_skip_measurements_previousAndThis = pd.DataFrame(
    {
        "y": {
            0: 250.0,
            1: 245.0,
            2: 40.0,
            3: 35.0,
            4: 30.0,
            5: 255.0,
        },
        "ds": {
            0: "2023-02-15 11:00:00",
            1: "2023-02-14 11:00:00",
            2: "2023-02-13 11:00:00",
            3: "2023-02-12 11:00:00",
            4: "2023-02-11 11:00:00",
            5: "2023-02-16 11:00:00",
        },
        "skipMeasurements": {
            0: None,
            1: "previousAndThis",
            2: "previousAndThis",
            3: None,
            4: None,
            5: nan,
        },
    }
)

test_prophet_model_skip_measurements_previousAndThis_expectation = pd.DataFrame(
    {
        "ds": {
            0: "2023-02-15 11:00:00",
            1: "2023-02-16 11:00:00",
        },
        "y": {0: 250.0, 1: 255},
    }
)

test_prophet_model_skip_measurements_previousAndThis_last_measurement = pd.DataFrame(
    {
        "y": {
            0: 250.0,
            1: 245.0,
            2: 40.0,
            3: 35.0,
            4: 30.0,
            5: 255.0,
        },
        "ds": {
            0: "2023-02-15 11:00:00",
            1: "2023-02-14 11:00:00",
            2: "2023-02-13 11:00:00",
            3: "2023-02-12 11:00:00",
            4: "2023-02-11 11:00:00",
            5: "2023-02-16 11:00:00",
        },
        "skipMeasurements": {
            0: None,
            1: "previousAndThis",
            2: "previousAndThis",
            3: None,
            4: None,
            5: "previousAndThis",
        },
    }
)


test_feedback_processor_seasonality_skip_measurements = {
    "y": {
        0: 42.0,
        1: 41.0,
        2: 40.0,
        3: 35.0,
    },
    "ds": {
        0: "2023-03-06 11:00:00",
        1: "2023-03-05 11:00:00",
        2: "2023-03-04 11:00:00",
        3: "2023-03-03 11:00:00",
    },
    "feedback": {
        0: {
            "isCorrectlyClassified": False,
            "isAnomaly": False,
            "reason": "expectedWeeklySeasonality",
            "freeTextReason": None,
            "skipMeasurements": None,
        },
        1: {
            "isCorrectlyClassified": None,
            "isAnomaly": None,
            "reason": None,
            "freeTextReason": None,
            "skipMeasurements": "previousAndThis",
        },
        2: {
            "isCorrectlyClassified": None,
            "isAnomaly": None,
            "reason": None,
            "freeTextReason": None,
            "skipMeasurements": None,
        },
        3: {
            "isCorrectlyClassified": True,
            "isAnomaly": True,
            "reason": None,
            "freeTextReason": None,
            "skipMeasurements": None,
        },
    },
    "anomaly_probability": {
        0: 0.0,
        1: 0.0,
        2: 0.0,
        3: 0.0,
    },
    "anomaly_predicted_value": {
        0: 42.83252400162256,
        1: 40.95227600086607,
        2: 38.65839999718243,
        3: 35.780745001179646,
    },
}

test_feedback_processor_seasonality_skip_measurements_expectation = {
    "y": {0: 42.0, 1: 41.0, 2: 40.0, 3: 35.0},
    "ds": {
        0: "2023-03-06 11:00:00",
        1: "2023-03-05 11:00:00",
        2: "2023-03-04 11:00:00",
        3: "2023-03-03 11:00:00",
    },
    "skipMeasurements": {0: None, 1: "previousAndThis", 2: None, 3: None},
    "external_regressor": {
        0: -0.8325240016225592,
        1: 0,
        2: 0,
        3: 0,
    },
}

test_feedback_processor_prophet_model_skip_measurements_expectation = {
    "y": {
        0: 42.0,
    },
    "ds": {
        0: "2023-03-06 11:00:00",
    },
    "skipMeasurements": {
        0: None,
    },
    "external_regressor": {
        0: -0.8325240016225592,
    },
}

test_anomaly_detector_parsed_historical_measurements = pd.DataFrame(
    [
        {
            "id": "49d198f1-eda7-42ad-bd70-5e1789bdf122",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": pd.Timestamp("2022-04-20 15:05:30+0000", tz="UTC"),
        },
        {
            "id": "959e5167-39e0-481b-9939-8ff7393391a5",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": pd.Timestamp("2022-04-19 15:05:10+0000", tz="UTC"),
        },
        {
            "id": "efc8f472-3d74-4a9a-965f-de14dcf4b2a9",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 2.0,
            "dataTime": pd.Timestamp("2022-04-18 14:49:59+0000", tz="UTC"),
        },
        {
            "id": "42a2b60b-d932-411d-9cab-bf7c33a84c65",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 1.0,
            "dataTime": pd.Timestamp("2022-04-17 14:49:20+0000", tz="UTC"),
        },
        {
            "id": "3ef53638-04cc-4614-b587-a059a81a4c2f",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 1.0,
            "dataTime": pd.Timestamp("2022-04-16 14:47:44+0000", tz="UTC"),
        },
        {
            "id": "b7dd6e88-f7a0-42c4-87c1-0662eb0e2ce5",
            "identity": "metric-test-adventureworks-anomaly_detection_test-row_count",
            "value": 21.0,
            "dataTime": pd.Timestamp("2022-04-15 15:04:42+0000", tz="UTC"),
        },
    ]
)

test_anomaly_detector_parsed_historic_check_results = pd.DataFrame(
    [
        {
            "identity": None,
            "measurementId": "49d198f1-eda7-42ad-bd70-5e1789bdf122",
            "type": "anomalyDetection",
            "definition": None,
            "location": {"filePath": None, "line": None, "col": None},
            "metrics": None,
            "dataSource": None,
            "table": None,
            "partition": None,
            "column": None,
            "outcome": "pass",
            "diagnostics": {
                "value": 21.0,
                "fail": {"greaterThanOrEqual": 22.0356176742, "lessThanOrEqual": 20.151426527},
                "warn": {"greaterThanOrEqual": 21.8786017453, "lessThanOrEqual": 20.3084424559},
                "anomalyProbability": None,
                "anomalyPredictedValue": 20.8705163355,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "anomalyErrorMessage": "",
            },
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
        },
        {
            "identity": None,
            "measurementId": "7fd4f85b-37b6-46f7-b6b8-56af91b3f920",
            "type": "anomalyDetection",
            "definition": None,
            "location": {"filePath": None, "line": None, "col": None},
            "metrics": None,
            "dataSource": None,
            "table": None,
            "partition": None,
            "column": None,
            "outcome": "pass",
            "diagnostics": {
                "value": 2.0,
                "fail": {"greaterThanOrEqual": 17.8995453719, "lessThanOrEqual": -1.8928266513},
                "warn": {"greaterThanOrEqual": 16.2501810366, "lessThanOrEqual": -0.243462316},
                "anomalyProbability": None,
                "anomalyPredictedValue": 8.4675791859,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "anomalyErrorMessage": "",
            },
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
        },
        {
            "identity": None,
            "measurementId": "d926b795-cf98-4e96-9eea-bf688c86d773",
            "type": "anomalyDetection",
            "definition": None,
            "location": {"filePath": None, "line": None, "col": None},
            "metrics": None,
            "dataSource": None,
            "table": None,
            "partition": None,
            "column": None,
            "outcome": "pass",
            "diagnostics": {
                "value": 2.0,
                "fail": {"greaterThanOrEqual": 18.6063794618, "lessThanOrEqual": -2.5466032303},
                "warn": {"greaterThanOrEqual": 16.8436309041, "lessThanOrEqual": -0.7838546726},
                "anomalyProbability": None,
                "anomalyPredictedValue": 8.4675791859,
                "anomalyErrorSeverity": "warn",
                "anomalyErrorCode": "made_daily_keeping_last_point_only",
                "anomalyErrorMessage": "",
            },
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
        },
    ]
)

test_anomaly_detector_parsed_empty_historic_check_results = pd.DataFrame(
    [
        {
            "identity": None,
            "measurementId": None,
            "type": None,
            "definition": None,
            "location": {"filePath": None, "line": None, "col": None},
            "metrics": None,
            "dataSource": None,
            "table": None,
            "partition": None,
            "column": None,
            "outcome": None,
            "diagnostics": {
                "value": None,
                "fail": None,
                "warn": None,
                "anomalyProbability": None,
                "anomalyPredictedValue": None,
                "anomalyErrorSeverity": "pass",
                "anomalyErrorCode": "",
                "anomalyErrorMessage": "",
            },
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
        }
    ]
)

test_anomaly_detector_parsed_ad_measurements = pd.DataFrame(
    [
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-20 15:05:30"),
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
            "anomaly_probability": nan,
            "anomaly_predicted_value": 20.870516335508597,
        },
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-19 15:05:10"),
            "feedback": nan,
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
        },
        {
            "y": 2.0,
            "ds": pd.Timestamp("2022-04-18 14:49:59"),
            "feedback": nan,
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
        },
        {
            "y": 1.0,
            "ds": pd.Timestamp("2022-04-17 14:49:20"),
            "feedback": nan,
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
        },
        {
            "y": 1.0,
            "ds": pd.Timestamp("2022-04-16 14:47:44"),
            "feedback": nan,
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
        },
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-15 15:04:42"),
            "feedback": nan,
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
        },
    ]
)

test_empty_anomaly_detector_parsed_ad_measurements = pd.DataFrame(
    [
        {"y": 21.0, "ds": pd.Timestamp("2022-04-20 15:05:30"), "feedback": nan},
        {"y": 21.0, "ds": pd.Timestamp("2022-04-19 15:05:10"), "feedback": nan},
        {"y": 2.0, "ds": pd.Timestamp("2022-04-18 14:49:59"), "feedback": nan},
        {"y": 1.0, "ds": pd.Timestamp("2022-04-17 14:49:20"), "feedback": nan},
        {"y": 1.0, "ds": pd.Timestamp("2022-04-16 14:47:44"), "feedback": nan},
        {"y": 21.0, "ds": pd.Timestamp("2022-04-15 15:04:42"), "feedback": nan},
    ]
)

# Feedback processor tests
test_feedback_processor_feedback_processed_df = pd.DataFrame(
    [
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-20 15:05:30"),
            "feedback": {
                "isCorrectlyClassified": None,
                "isAnomaly": None,
                "reason": None,
                "freeTextReason": None,
                "skipMeasurements": None,
            },
            "anomaly_probability": nan,
            "anomaly_predicted_value": 20.870516335508597,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": 0.1294836644914028,
        },
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-19 15:05:10"),
            "feedback": {},
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": nan,
        },
        {
            "y": 2.0,
            "ds": pd.Timestamp("2022-04-18 14:49:59"),
            "feedback": {},
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": nan,
        },
        {
            "y": 1.0,
            "ds": pd.Timestamp("2022-04-17 14:49:20"),
            "feedback": {},
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": nan,
        },
        {
            "y": 1.0,
            "ds": pd.Timestamp("2022-04-16 14:47:44"),
            "feedback": {},
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": nan,
        },
        {
            "y": 21.0,
            "ds": pd.Timestamp("2022-04-15 15:04:42"),
            "feedback": {},
            "anomaly_probability": nan,
            "anomaly_predicted_value": nan,
            "isCorrectlyClassified": True,
            "isAnomaly": nan,
            "reason": "Invalid reason",
            "freeTextReason": nan,
            "skipMeasurements": nan,
            "is_misclassification": False,
            "predicted_to_real_delta": nan,
        },
    ]
)

df_prophet_model_setup_fit_predict = pd.DataFrame(
    [
        {
            "ds": pd.Timestamp("2024-01-01 00:00:00"),
            "yhat": 16.21464558636365,
            "yhat_lower": 14.32393315860764,
            "yhat_upper": 18.121126003200843,
        },
        {
            "ds": pd.Timestamp("2024-01-02 00:00:00"),
            "yhat": 16.090679703179294,
            "yhat_lower": 13.735803504108135,
            "yhat_upper": 18.440984207591086,
        },
        {
            "ds": pd.Timestamp("2024-01-03 00:00:00"),
            "yhat": 15.96671381996878,
            "yhat_lower": 13.915890607829295,
            "yhat_upper": 18.314768039840203,
        },
        {
            "ds": pd.Timestamp("2024-01-04 00:00:00"),
            "yhat": 15.84274793682604,
            "yhat_lower": 13.609051272889884,
            "yhat_upper": 17.869056388352494,
        },
        {
            "ds": pd.Timestamp("2024-01-05 00:00:00"),
            "yhat": 15.718782053683304,
            "yhat_lower": 13.788612334052011,
            "yhat_upper": 17.746732131697943,
        },
    ]
)
