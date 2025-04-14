from textwrap import dedent

from dotenv import load_dotenv
from soda_core.common.logging_configuration import configure_logging
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def main():
    print("Verifying contract on agent")
    configure_logging()

    project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/components/manual_test_agent_flow.py")]
    load_dotenv(f"{project_root_dir}/.env", override=True)

    contract_yaml_str: str = dedent(
        """
        data_source: bus_nienu
        dataset_prefix: [nyc, public]
        dataset: bus_breakdown_and_delays
        columns:
          - name: reason
            checks:
              - invalid:
                  valid_values: [ 'Heavy Traffic', 'Other', 'Mechanical Problem', 'Won`t Start', 'Problem Run' ]
        checks:
          - schema:
    """
    ).strip()

    soda_cloud_yaml_str = dedent(
        """
        soda_cloud:
          bla: bla
    """
    ).strip()

    ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml_str)],
        soda_cloud_yaml_source=SodaCloudYamlSource.from_str(soda_cloud_yaml_str),
        soda_cloud_use_agent=True,
        soda_cloud_use_agent_blocking_timeout_in_minutes=55,
    )


if __name__ == "__main__":
    main()
