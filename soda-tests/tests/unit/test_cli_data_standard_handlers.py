"""Unit tests for the ``soda data-standard verify`` CLI plumbing.

Two layers covered:
  - ``handle_verify_data_standards`` (handler): the lazy ``verify_data_standards``
    import is mocked so no real engine runs; we assert the right yaml sources,
    data source impls, and flags reach the engine call.
  - ``create_cli_parser`` → argparse dispatch: confirms the new subcommand
    parses ``-c``/``-ds``/etc. and routes through to the handler.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_standard import (
    handle_verify_data_standards,
    interpret_check_collection_session_result,
)


@pytest.mark.parametrize(
    "has_errors, is_failed, is_warned, cloud_failed, expected_exit_code",
    [
        (False, False, False, False, ExitCode.OK),
        (False, False, True, False, ExitCode.CHECK_WARNINGS),
        (False, True, False, False, ExitCode.CHECK_FAILURES),
        (True, False, False, False, ExitCode.LOG_ERRORS),
        (False, False, False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    ],
)
def test_interpret_check_collection_session_result_exit_codes(
    has_errors, is_failed, is_warned, cloud_failed, expected_exit_code
):
    """Mirror of ``interpret_contract_verification_result`` for the universal
    ``CheckCollectionSessionResult``."""
    item = MagicMock()
    type(item).has_errors = PropertyMock(return_value=has_errors)
    type(item).is_failed = PropertyMock(return_value=is_failed)
    type(item).is_warned = PropertyMock(return_value=is_warned)
    item.sending_results_to_soda_cloud_failed = cloud_failed

    session = MagicMock()
    session.results = [item]

    assert interpret_check_collection_session_result(session) == expected_exit_code


@patch("soda_core.cli.handlers.data_standard.DataSourceImpl.from_yaml_source")
@patch("soda_core.cli.handlers.data_standard._create_datasource_yamls")
def test_handle_verify_data_standards_dispatches_to_engine(mock_create_ds_yamls, mock_ds_from_yaml):
    """The handler forwards yaml sources / data source impl / publish flag to
    ``soda_data_standard.api.verify_data_standards`` (the lazy import)."""
    mock_create_ds_yamls.return_value = [MagicMock(file_path="ds.yml")]
    fake_ds = MagicMock()
    fake_ds.name = "fake_ds"
    mock_ds_from_yaml.return_value = fake_ds

    fake_module = MagicMock()
    session = MagicMock()
    session.results = []
    fake_module.verify_data_standards = MagicMock(return_value=session)

    with patch.dict(sys.modules, {"soda_data_standard.api": fake_module}):
        exit_code = handle_verify_data_standards(
            check_file_paths=["a.yml", "b.yml"],
            data_source_file_paths=["ds.yml"],
            soda_cloud_file_path=None,
            variables={"k": "v"},
            publish=False,
            verbose=False,
            check_paths=None,
            check_selectors=[],
            diagnostics_warehouse_file_path=None,
        )

    assert exit_code == ExitCode.OK
    fake_module.verify_data_standards.assert_called_once()
    kwargs = fake_module.verify_data_standards.call_args.kwargs
    assert len(kwargs["yaml_sources"]) == 2
    assert [s.file_path for s in kwargs["yaml_sources"]] == ["a.yml", "b.yml"]
    assert kwargs["data_source_impl"] is fake_ds
    assert kwargs["publish_results"] is False
    assert kwargs["variables"] == {"k": "v"}
    # Single data source → no all_data_source_impls dict needed
    assert kwargs["all_data_source_impls"] is None


@patch("soda_core.cli.handlers.data_standard._create_datasource_yamls")
def test_handle_verify_data_standards_returns_log_errors_when_extension_missing(mock_create_ds_yamls):
    """ImportError on the lazy ``soda_data_standard.api`` import must yield
    a LOG_ERRORS exit, not a traceback."""
    mock_create_ds_yamls.return_value = [MagicMock(file_path="ds.yml")]

    with patch(
        "soda_core.cli.handlers.data_standard.DataSourceImpl.from_yaml_source",
        return_value=MagicMock(name="fake_ds"),
    ):
        # Force the lazy import to fail by removing the entry from sys.modules
        # and ensuring no real package exists at import-time.
        sys.modules.pop("soda_data_standard.api", None)

        def _raise_import(name, *a, **kw):
            if name == "soda_data_standard.api":
                raise ImportError("not installed")
            return __import__(name, *a, **kw)

        with patch("builtins.__import__", side_effect=_raise_import):
            exit_code = handle_verify_data_standards(
                check_file_paths=["a.yml"],
                data_source_file_paths=["ds.yml"],
                soda_cloud_file_path=None,
                variables=None,
                publish=False,
                verbose=False,
                check_paths=None,
                check_selectors=[],
                diagnostics_warehouse_file_path=None,
            )

    assert exit_code == ExitCode.LOG_ERRORS


@patch("soda_core.cli.cli.handle_verify_data_standards")
def test_cli_argument_mapping_for_data_standard_verify_command(mock_handler):
    """Argparse parses ``soda data-standard verify`` into the right kwargs."""
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-standard",
        "verify",
        "-c",
        "a.yml",
        "b.yml",
        "-ds",
        "ds.yml",
        "-sc",
        "cloud.yml",
        "-p",
        "-v",
        "--set",
        "k=v",
        "--check-paths",
        "c.path.one",
        "c.path.two",
        "-dw",
        "dw.yml",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(
        check_file_paths=["a.yml", "b.yml"],
        data_source_file_paths=["ds.yml"],
        soda_cloud_file_path="cloud.yml",
        variables={"k": "v"},
        publish=True,
        verbose=True,
        check_paths=["c.path.one", "c.path.two"],
        check_selectors=[],
        diagnostics_warehouse_file_path="dw.yml",
    )
