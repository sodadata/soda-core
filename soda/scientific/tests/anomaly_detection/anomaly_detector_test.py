import logging

import pytest
from soda.common.logs import Logs

LOGS = Logs(logging.getLogger(__name__))


@pytest.mark.parametrize(
    "historical_measurements, historical_check_results, expectation",
    [
        pytest.param(
            {
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
            },
            {
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
                            "anomalyProbability": 0.0,
                            "anomalyPredictedValue": 20.870516335508597,
                            "anomalyErrorSeverity": "warn",
                            "anomalyErrorCode": "made_daily_keeping_last_point_only",
                            "fail": {"lessThanOrEqual": 20.15142652696, "greaterThanOrEqual": 22.03561767424},
                            "warn": {"lessThanOrEqual": 20.3084424559, "greaterThanOrEqual": 21.8786017453},
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
                            "anomalyProbability": 0.0,
                            "anomalyPredictedValue": 8.46757918589978,
                            "anomalyErrorSeverity": "warn",
                            "anomalyErrorCode": "made_daily_keeping_last_point_only",
                            "fail": {"lessThanOrEqual": -1.89282665126, "greaterThanOrEqual": 17.89954537186},
                            "warn": {"lessThanOrEqual": -0.243462316, "greaterThanOrEqual": 16.2501810366},
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
                            "anomalyProbability": 0.0,
                            "anomalyPredictedValue": 8.46757918589978,
                            "anomalyErrorSeverity": "warn",
                            "anomalyErrorCode": "made_daily_keeping_last_point_only",
                            "fail": {"lessThanOrEqual": -2.54660323027, "greaterThanOrEqual": 18.60637946177},
                            "warn": {"lessThanOrEqual": -0.7838546726, "greaterThanOrEqual": 16.8436309041},
                        },
                    },
                ]
            },
            {
                "value": 21.0,
                "fail": {"greaterThanOrEqual": 42.59848061337, "lessThanOrEqual": -10.97031870027},
                "warn": {"greaterThanOrEqual": 38.1344140039, "lessThanOrEqual": -6.5062520908},
                "anomalyProbability": 0.0,
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
            },
        )
    ],
)
def test_anomaly_detector_evaluate(historical_measurements, historical_check_results, expectation):
    from soda.scientific.anomaly_detection.anomaly_detector import AnomalyDetector

    detector = AnomalyDetector(historical_measurements, historical_check_results, logs=LOGS)
    _, diagnostic = detector.evaluate()
    assert diagnostic["value"] == expectation["value"]
    assert diagnostic["anomalyProbability"] == pytest.approx(expectation["anomalyProbability"])
    assert diagnostic["anomalyPredictedValue"] == pytest.approx(expectation["anomalyPredictedValue"])
