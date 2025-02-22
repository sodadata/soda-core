from textwrap import dedent

from dotenv import load_dotenv

from conftest import configure_logging
from soda_core.contracts.contract_verification import ContractVerification, ContractVerificationResult


def main():
    print("Verifying contract on agent")
    configure_logging()

    project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/components/manual_test_agent_flow.py")]
    load_dotenv(f"{project_root_dir}/.env", override=True)

    contract_yaml_str: str = dedent("""
        data_source: milan_throwaway
        dataset_prefix: [nyc, public]
        dataset: bus_breakdown_and_delays
        columns:
          - name: reason
            valid_values: [ 'Heavy Traffic', 'Other', 'Mechanical Problem', 'Won`t Start', 'Problem Run' ]
            checks:
              - type: invalid_count
        checks:
          - type: schema
    """).strip()

    soda_cloud_yaml_str = dedent("""
        soda_cloud:
          bla: bla
    """).strip()

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_soda_cloud_yaml_str(soda_cloud_yaml_str)
        .with_contract_yaml_str(contract_yaml_str)
        .with_execution_on_soda_agent()
        .execute()
     )


if __name__ == "__main__":
    main()
