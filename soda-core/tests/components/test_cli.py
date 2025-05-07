import sys
from unittest.mock import patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.soda import create_cli_parser

# from soda_core.cli.soda import CLI


@pytest.mark.parametrize(
    "args, expected",
    [
        (
            [
                "soda",
                "contract",
                "verify",
                "-c",
                "a.yaml",
                "b.yaml",
                "-d",
                "some/remote/dataset/identifier",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "-a",
                "-btm",
                "42",
                "-p",
                "-v",
                "--set",
                "key1=value1",
                "--set",
                "key2=value2",
            ],
            [
                ["a.yaml", "b.yaml"],
                ["some/remote/dataset/identifier"],
                "ds.yaml",
                {"key1": "value1", "key2": "value2"},
                True,
                True,
                42,
                None,
            ],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
                "-a",
                "-btm",
                "42",
                "-p",
                "-v",
                "--set",
                "key1=value1",
            ],
            [
                None,
                ["some-dataset"],
                "ds.yaml",
                {"key1": "value1"},
                True,
                True,
                42,
                None,
            ],
        ),
        (
            [
                "soda",
                "contract",
                "verify",
                "-d",
                "some-dataset",
                "-ds",
                "ds.yaml",
                "-sc",
                "cloud.yaml",
            ],
            [
                None,
                ["some-dataset"],
                "ds.yaml",
                {},
                False,
                False,
                60,
                None
            ],
        ),
    ],
)
@patch("soda_core.cli.soda.handle_verify_contract")
def test_cli_argument_mapping_for_contract_verify_command(mock_handler, args, expected):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = args

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(*expected)


def test_verify_command_raises_exception_when_none_of_contract_or_dataset_specified():
    sys.argv = [
        "soda",
        "contract",
        "verify",
        "-ds",
        "ds.yaml",
        "-sc",
        "cloud.yaml",
        "-a",
        "-btm",
        "42",
        "-p",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()
    with pytest.raises(SystemExit) as e:
        _ = args.handler_func(args)

    assert ExitCode.LOG_ERRORS == e.value.code


def test_verify_command_raises_exception_when_variables_are_incorrectly_formatted():
    sys.argv = [
        "soda",
        "contract",
        "verify",
        "-ds",
        "ds.yaml",
        "-sc",
        "cloud.yaml",
        "--set",
        "invalid_variable",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()
    with pytest.raises(SystemExit) as e:
        _ = args.handler_func(args)

    assert ExitCode.LOG_ERRORS == e.value.code


@patch("soda_core.cli.soda.handle_publish_contract")
def test_cli_argument_mapping_for_contract_publish_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "contract",
        "publish",
        "-c",
        "a.yaml",
        "b.yaml",
        "-sc",
        "cloud.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(
        ["a.yaml", "b.yaml"],
        "cloud.yaml",
    )


@patch("soda_core.cli.soda.handle_test_contract")
def test_cli_argument_mapping_for_contract_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "contract",
        "test",
        "-c",
        "a.yaml",
        "b.yaml",
        "-ds",
        "ds.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with(
        ["a.yaml", "b.yaml"],
        {},
        "ds.yaml",
    )


@patch("soda_core.cli.soda.handle_create_data_source")
def test_cli_argument_mapping_for_data_source_create_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-source",
        "create",
        "-f",
        "ds.yaml",
        "-t",
        "postgres",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("ds.yaml", "postgres")


@patch("soda_core.cli.soda.handle_test_data_source")
def test_cli_argument_mapping_for_data_source_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "data-source",
        "test",
        "-ds",
        "ds.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("ds.yaml")


@patch("soda_core.cli.soda.handle_create_soda_cloud")
def test_cli_argument_mapping_for_soda_cloud_create_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "cloud",
        "create",
        "-f",
        "sc.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("sc.yaml")


@patch("soda_core.cli.soda.handle_test_soda_cloud")
def test_cli_argument_mapping_for_soda_cloud_test_command(mock_handler):
    mock_handler.return_value = ExitCode.OK.value
    sys.argv = [
        "soda",
        "cloud",
        "test",
        "-sc",
        "sc.yaml",
        "-v",
    ]

    parser = create_cli_parser()
    args = parser.parse_args()

    with pytest.raises(SystemExit) as e:
        args.handler_func(args)

    assert e.value.code == 0

    mock_handler.assert_called_once_with("sc.yaml")


# class CLI4Test(CLI):
#     def __init__(self, argv: list[str]):
#         super().__init__()
#         self.contract_file_paths: Optional[list[str]] = None
#         self.data_source_file_path: Optional[str] = None
#         self.soda_cloud_file_path: Optional[str] = None
#         self.skip_publish: Optional[bool] = None
#         self.use_agent: Optional[bool] = None
#         self.exit_code: int = 0
#         self.argv: list[str] = argv
#         self.contract_verification_result: Optional[ContractVerificationSessionResult] = None
#
#     def execute(self) -> None:
#         sys.argv = self.argv
#         super().execute()
#
#     def _verify_contract(
#         self,
#         contract_file_paths: Optional[list[str]],
#         data_source_file_path: Optional[str],
#         soda_cloud_file_path: Optional[str],
#         skip_publish: bool,
#         use_agent: bool,
#         blocking_timeout_in_minutes: int,
#     ):
#         self.contract_file_paths = contract_file_paths
#         self.data_source_file_path = data_source_file_path
#         self.soda_cloud_file_path = soda_cloud_file_path
#         self.skip_publish = skip_publish
#         self.use_agent = use_agent
#
#         self.contract_verification_result = super()._verify_contract(
#             contract_file_paths=contract_file_paths,
#             data_source_file_path=data_source_file_path,
#             soda_cloud_file_path=soda_cloud_file_path,
#             skip_publish=skip_publish,
#             use_agent=use_agent,
#             blocking_timeout_in_minutes=blocking_timeout_in_minutes,
#         )
#
#     def _configure_logging(self, verbose: bool) -> None:
#         pass
#
#     def _exit_with_code(self, exit_code: int) -> None:
#         self.exit_code = exit_code
#
#     def _create_argument_parser(self, epilog: str) -> ArgumentParser:
#         return ArgumentParser4Test(epilog=epilog)
#
#
# class ArgumentParser4Test(ArgumentParser):
#     def exit(self, status=0, message=None):
#         print(f"Skipping exit in unit test status={status}, message={message}")
#
#
# test_table_specification = (
#     TestTableSpecification.builder()
#     .table_purpose("cli")
#     .column_text("id")
#     .rows(
#         rows=[
#             ("1",),
#             ("2",),
#             ("3",),
#         ]
#     )
#     .build()
# )
#
#
# def test_cli(data_source_test_helper: DataSourceTestHelper):
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     contract_yaml_str: str = dedent_and_strip(
#         f"""
#         data_source: postgres_test_ds
#         dataset: {test_table.unique_name}
#         dataset_prefix: {data_source_test_helper.dataset_prefix}
#         columns:
#           - name: id
#         checks:
#           - row_count:
#               threshold:
#                 must_be: 3
#           - schema:
#     """
#     )
#
#     data_source_yaml_str: str = dedent_and_strip(
#         """
#         type: postgres
#         name: postgres_test_ds
#         connection:
#           host: localhost
#           user: soda_test
#           database: soda_test
#     """
#     )
#
#     contract_tmp_file = tempfile.NamedTemporaryFile()
#     with open(contract_tmp_file.name, "w") as f:
#         f.write(contract_yaml_str)
#
#     data_source_tmp_file = tempfile.NamedTemporaryFile()
#     with open(data_source_tmp_file.name, "w") as f:
#         f.write(data_source_yaml_str)
#
#     test_cli: CLI4Test = CLI4Test(["soda", "contract", "verify", "-ds", data_source_tmp_file.name, "-c", contract_tmp_file.name])
#     test_cli.execute()
#     assert test_cli.exit_code == 0
#     assert test_cli.contract_verification_result.is_ok()
#     contract_verification_result: ContractVerificationResult = test_cli.contract_verification_result.contract_results[0]
#     assert len(contract_verification_result.check_results) == 2
#
#
# def test_cli_wrong_pwd(data_source_test_helper: DataSourceTestHelper):
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     contract_yaml_str: str = dedent_and_strip(
#         f"""
#         data_source: postgres_test_ds
#         dataset: {test_table.unique_name}
#         dataset_prefix: {data_source_test_helper.dataset_prefix}
#         columns:
#           - name: id
#         checks:
#           - row_count:
#               threshold:
#                 must_be: 3
#           - schema:
#     """
#     )
#
#     data_source_yaml_str: str = dedent_and_strip(
#         """
#         type: postgres
#         name: postgres_test_ds
#         connection:
#           host: localhost
#           user: wrongpwd!
#           database: soda_test
#     """
#     )
#
#     contract_tmp_file = tempfile.NamedTemporaryFile()
#     with open(contract_tmp_file.name, "w") as f:
#         f.write(contract_yaml_str)
#
#     data_source_tmp_file = tempfile.NamedTemporaryFile()
#     with open(data_source_tmp_file.name, "w") as f:
#         f.write(data_source_yaml_str)
#
#     test_cli: CLI4Test = CLI4Test(["soda", "contract", "verify", "-ds", data_source_tmp_file.name, "-c", contract_tmp_file.name])
#     test_cli.execute()
