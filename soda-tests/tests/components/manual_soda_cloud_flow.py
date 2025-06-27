from textwrap import dedent

from dotenv import load_dotenv
from soda_core.common.logging_configuration import configure_logging
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def main():
    print("Verifying contract and sending results to Soda Cloud")
    configure_logging(verbose=True)

    project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/components/manual_soda_cloud_flow.py")]
    load_dotenv(f"{project_root_dir}/.env", override=True)

    data_source_yaml_str: str = dedent(f"""
        name: toms_local_postgres
        type: postgres
        connection:
            host: localhost
            user: soda_test
            password: <PASSWORD>
            database: soda_test
    """).strip()

    contract_yaml_str: str = dedent(
        """
        dataset: toms_local_postgres/soda_test/dev_tom/ecommerce_orders
        variables:
          start_timestamp:
            default: DATE_TRUNC('week', CAST('${soda.NOW}' AS TIMESTAMP))
          end_timestamp:
            default: DATE_TRUNC('week', CAST('${soda.NOW}' AS TIMESTAMP)) + INTERVAL '30 days'
        # failed_rows:
        # strategy: store_keys
        #   keys: ['order_id']
        checks:
          - schema:
          - freshness:
              column: created_at
              threshold:
                must_be_less_than: 2
        columns:
          - name: order_id
            data_type: integer
            checks:
              - missing:
                  name: Must not have null values
          - name: customer_id
            data_type: integer
            checks:
              - missing:
                  name: Must not have null values
          - name: order_date
            data_type: date
            checks:
              - missing:
                  name: Must not have null values
              - failed_rows:
                  name: Cannot be in the future
                  expression: order_date > DATE_TRUNC('day', CAST('${soda.NOW} ' AS TIMESTAMP)) +
                    INTERVAL '1 day'
                  threshold:
                    must_be: 0
          - name: region
            data_type: VARCHAR
            checks:
              - invalid:
                  valid_values:
                    - North
                    - South
                    - East
                    - West
                  name: Valid values
          - name: product_category
            data_type: VARCHAR
          - name: quantity
            data_type: integer
            checks:
              - missing:
                  name: Must not have null values
              - invalid:
                  valid_min: 0
                  name: Must be higher than 0
          - name: price
            data_type: numeric
            checks:
              - invalid:
                  valid_min: 0
                  name: Must be higher than 0
              - missing:
                  name: Must not have null values
              - metric:
                  expression: |
                    AVG(price - quantity)
                  threshold:
                    must_be_between:
                      greater_than_or_equal: 0
                      less_than_or_equal: 10000
          - name: payment_method
            data_type: VARCHAR
            checks:
              - missing:
                  name: Must not have null values
              - invalid:
                  threshold:
                    metric: count
                    must_be: 0
                  filter: region <> 'north'
                  valid_values:
                    - PayPal
                    - Bank Transfer
                    - Cash
                    - Credit Card
                  name: Valid values in all regions except North
              - invalid:
                  name: Valid values in North
                  filter: region = 'north'
                  valid_values:
                    - PayPal
                    - Bank Transfer
                    - Credit Card
                  qualifier: ABC124
    """
    ).strip()

    soda_cloud_yaml_str = dedent(
        """
        soda_cloud:
          host: ${env.SODA_CLOUD_HOST}
          api_key_id: ${env.SODA_CLOUD_API_KEY_ID}
          api_key_secret: ${env.SODA_CLOUD_API_KEY_SECRET}
    """
    ).strip()

    soda_cloud: SodaCloud = SodaCloud.from_yaml_source(
        SodaCloudYamlSource.from_str(soda_cloud_yaml_str),
        provided_variable_values={}
    )

    ContractVerificationSession.execute(
        data_source_yaml_sources=[DataSourceYamlSource.from_str(data_source_yaml_str)],
        contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml_str, file_path="orders.yml")],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True
    )


if __name__ == "__main__":
    main()
