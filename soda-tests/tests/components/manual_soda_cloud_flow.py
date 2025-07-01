from textwrap import dedent

from dotenv import load_dotenv

from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_configuration import configure_logging
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def test_manual_soda_cloud_flow_ecommerce_orders_contract_and_results_upload_to_soda_cloud(
    data_source_test_helper: DataSourceTestHelper
) -> None:

    print("Verifying contract and sending results to Soda Cloud")
    configure_logging(verbose=True)

    project_root_dir = __file__[: -len("/soda-core/tests/soda_core/tests/components/manual_soda_cloud_flow.py")]
    load_dotenv(f"{project_root_dir}/.env", override=True)

    data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(
        data_source_yaml_source=data_source_test_helper._create_data_source_yaml_source()
    )

    data_source_name: str = data_source_test_helper.data_source_impl.name
    database_name: str = data_source_test_helper.dataset_prefix[0]
    schema_name: str = data_source_test_helper.dataset_prefix[1]
    qualified_table_name: str = f'{schema_name}."ecommerce_orders"'

    contract_yaml_str: str = dedent(
        f"""
        dataset: {data_source_name}/{database_name}/{schema_name}/ecommerce_orders
        variables:
          start_timestamp:
            default: DATE_TRUNC('week', CAST('${{soda.NOW}}' AS TIMESTAMP))
          end_timestamp:
            default: DATE_TRUNC('week', CAST('${{soda.NOW}}' AS TIMESTAMP)) + INTERVAL '30 days'
        failed_rows:
          strategy: store_keys
          keys: ['order_id']
        diagnostics: store_check_results
        checks:
          - schema:
          - row_count:
          - row_count:
              qualifier: v2
              name: Filtered row count
              filter: order_date < DATE_TRUNC('week', CAST('${{soda.NOW}}' AS TIMESTAMP)) + INTERVAL '30 days'
          - freshness:
              column: created_at
              threshold:
                must_be_less_than: 2
          - duplicate:
              name: Multi column duplicate
              columns: ['customer_id', 'order_date']
          - metric:
              name: Enough avg quantity
              expression: AVG("quantity")
              threshold:
                must_be_less_than: 3
          - metric:
              name: Enough East orders
              qualifier: v2
              query: SELECT COUNT(*) FROM {qualified_table_name} WHERE "region" = 'East'
              threshold:
                must_be_greater_than: 3
          - failed_rows:
              name: Quantity greater than 1
              expression: |
                "quantity" > 1
              threshold:
                must_be_less_than: 3
          - failed_rows:
              name: No bank transfers for bad customer
              qualifier: v2
              query: |
                SELECT *
                FROM {qualified_table_name}
                WHERE "customer_id" = '242' and "payment_method" = 'Bank Transfer'
              threshold:
                must_be_greater_than: 3

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
                  expression: |
                    order_date > DATE_TRUNC('day', CAST('${{soda.NOW}} ' AS TIMESTAMP)) + INTERVAL '1 day'
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
              - aggregate:
                  function: avg
                  threshold:
                    must_be_between:
                      greater_than: 20
                      less_than: 50
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
                  qualifier: v2
    """
    ).strip()

    print(f"contract_yaml_str: \n{contract_yaml_str}")

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

    with data_source_impl:
        drop_ecommerce_orders_table(data_source_impl=data_source_impl, schema=schema_name)
        create_ecommerce_orders_table(data_source_impl=data_source_impl, schema=schema_name)
        insert_ecommerce_orders_data(data_source_impl=data_source_impl, schema=schema_name)

        ContractVerificationSession.execute(
            data_source_impls=[data_source_impl],
            contract_yaml_sources=[ContractYamlSource.from_str(contract_yaml_str, file_path="orders.yml")],
            soda_cloud_impl=soda_cloud,
            soda_cloud_publish_results=True
        )


def drop_ecommerce_orders_table(data_source_impl: DataSourceImpl, schema: str):
    data_source_impl.execute_update(f"""
        drop table if exists {schema}.ecommerce_orders;
    """)


def create_ecommerce_orders_table(data_source_impl: DataSourceImpl, schema: str):
    data_source_impl.execute_update(f"""
        create table {schema}.ecommerce_orders
        (
            order_id         integer not null primary key,
            customer_id      integer not null,
            order_date       date    not null,
            region           varchar(50),
            product_category varchar(100),
            quantity         integer,
            price            numeric(10, 2),
            payment_method   varchar(50),
            is_fraud         boolean,
            vat              numeric,
            created_at       timestamp
        );
    """)


def insert_ecommerce_orders_data(data_source_impl, schema: str):
    data_source_impl.execute_update(f"""
        insert into {schema}.ecommerce_orders (order_id, customer_id, order_date, region, product_category, quantity, price, payment_method, is_fraud, vat, created_at) values
        (2000, 213, '2025-05-01', 'North', 'Clothing', 2, 480.51, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2001, 229, '2025-05-02', 'East', 'Books', 5, 190.47, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (2002, 242, '2025-05-03', 'West', 'Books', 1, 473.99, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2003, 230, '2025-05-04', 'East', 'Electronics', 1, 67.56, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (2004, 235, '2025-05-05', 'South', 'Clothing', 1, 362.29, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2005, 246, '2025-05-06', 'West', 'Clothing', 5, 10.91, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2006, 211, '2025-05-07', 'East', 'Home', 1, 107.96, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2007, 200, '2025-05-08', 'South', 'Clothing', 5, 250.0, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2008, 226, '2025-05-09', 'East', 'Home', 4, 205.4, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2009, 202, '2025-05-10', 'West', 'Books', 4, 215.62, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2010, 226, '2025-05-11', 'East', 'Home', 1, 200.94, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2011, 216, '2025-05-12', 'North', 'Books', 3, 11.28, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2012, 223, '2025-05-13', 'South', 'Clothing', 3, 470.68, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2013, 201, '2025-05-14', 'South', 'Home', 4, 391.92, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2014, 209, '2025-05-15', 'East', 'Clothing', 2, 333.96, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2015, 222, '2025-05-16', 'North', 'Electronics', 4, 197.07, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2016, 246, '2025-05-17', 'South', 'Home', 1, 170.49, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2017, 226, '2025-05-18', 'North', 'Home', 3, 294.1, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2018, 210, '2025-05-19', 'South', 'Clothing', 5, 275.96, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (2019, 207, '2025-05-20', 'West', 'Clothing', 3, 277.04, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2020, 222, '2025-05-21', 'East', 'Electronics', 2, 146.26, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2021, 245, '2025-05-22', 'North', 'Electronics', 4, 161.95, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2022, 205, '2025-05-23', 'North', 'Electronics', 3, 470.65, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (2023, 225, '2025-05-24', 'West', 'Books', 2, 387.2, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2024, 203, '2025-05-25', 'North', 'Clothing', 2, 339.94, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2025, 244, '2025-05-26', 'North', 'Home', 4, 60.96, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2026, 235, '2025-05-27', 'North', 'Books', 5, 483.34, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2027, 223, '2025-05-28', 'South', 'Clothing', 4, 294.11, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2028, 209, '2025-05-29', 'West', 'Electronics', 2, 329.78, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2029, 222, '2025-05-30', 'East', 'Home', 5, 199.05, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (2030, 231, '2025-05-31', 'South', 'Home', 1, 51.12, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2031, 210, '2025-06-01', 'North', 'Home', 3, 228.68, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2032, 210, '2025-06-02', 'South', 'Clothing', 5, 15.69, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2033, 241, '2025-06-03', 'North', 'Clothing', 2, 221.7, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (2034, 234, '2025-06-04', 'East', 'Electronics', 4, 89.14, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2035, 233, '2025-06-05', 'West', 'Clothing', 3, 51.73, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2036, 250, '2025-06-06', 'North', 'Electronics', 5, 97.01, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (2037, 229, '2025-06-07', 'South', 'Home', 2, 61.79, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2038, 235, '2025-06-08', 'West', 'Home', 4, 176.71, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2039, 225, '2025-06-09', 'North', 'Electronics', 5, 93.96, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2040, 229, '2025-06-10', 'West', 'Books', 1, 326.69, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2041, 212, '2025-06-11', 'South', 'Electronics', 4, 407.18, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2042, 249, '2025-06-12', 'South', 'Clothing', 2, 208.88, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2043, 201, '2025-06-13', 'West', 'Clothing', 5, 219.29, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2044, 223, '2025-06-14', 'East', 'Home', 1, 263.56, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2045, 232, '2025-06-15', 'North', 'Home', 4, 304.68, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2046, 211, '2025-06-16', 'West', 'Electronics', 1, 19.52, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2047, 239, '2025-06-17', 'South', 'Clothing', 3, 180.71, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2048, 247, '2025-06-18', 'North', 'Clothing', 5, 396.36, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (2049, 245, '2025-05-06', 'North', 'Home', 1, 461.85, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (2050, 246, '2025-06-20', 'West', 'Clothing', 1, 215.2, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2051, 208, '2025-06-21', 'South', 'Home', 4, 299.66, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2052, 239, '2025-06-22', 'South', 'Clothing', 4, 331.51, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2053, 208, '2025-06-23', 'West', NULL, 3, 371.26, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2054, 201, '2025-06-24', 'Unknown', 'Books', 2, 391.66, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (2055, 200, '2025-06-25', 'East', 'Home', 1, -355.77, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (2056, 200, '2025-06-26', 'South', 'Home', 4, 100.72, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (2057, 208, '2025-06-27', 'West', 'Electronics', 2, 392.26, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2058, 239, '2025-06-28', 'West', 'Electronics', 3, 419.72, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (2059, 210, '2025-06-29', 'East', 'Clothing', 1, 437.16, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (2060, 242, '2025-06-30', 'West', 'Home', 5, 452.46, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (3000, 209, '2025-05-14', 'East', 'Books', 3, 258.64, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3001, 244, '2025-05-25', 'West', 'Books', 1, 420.73, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (3002, 233, '2025-05-05', 'South', 'Books', 4, 70.03, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3003, 210, '2025-05-03', 'East', 'Clothing', 1, 302.94, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3004, 225, '2025-06-24', 'East', 'Books', 1, 17.53, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3005, 204, '2025-06-14', 'East', 'Home', 2, 265.46, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3006, 226, '2025-06-27', 'East', 'Electronics', 5, 174.69, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3007, 237, '2025-05-05', 'East', 'Home', 3, 127.04, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3008, 230, '2025-06-07', 'East', 'Home', 2, 479.9, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (3009, 203, '2025-06-05', 'East', 'Clothing', 1, 60.68, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3010, 221, '2025-05-30', 'West', 'Books', 3, 35.93, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3011, 202, '2025-05-26', 'West', 'Electronics', 3, 427.3, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3012, 200, '2025-05-06', 'West', 'Books', 1, 215.73, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3013, 229, '2025-06-08', 'South', 'Electronics', 2, 252.02, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3014, 232, '2025-05-28', 'South', 'Home', 1, 411.32, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (3015, 225, '2025-06-28', 'North', 'Clothing', 4, 461.51, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (3016, 222, '2025-06-23', 'West', 'Clothing', 3, 369.25, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (3017, 240, '2025-06-28', 'West', 'Home', 5, 148.95, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3018, 233, '2025-05-25', 'North', 'Home', 3, 371.64, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3019, 235, '2025-05-30', 'West', 'Electronics', 3, 459.13, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (3020, 200, '2025-06-15', 'West', 'Home', 2, 145.75, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (3021, 237, '2025-06-19', 'South', 'Books', 2, 204.13, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3022, 210, '2025-06-09', 'East', 'Home', 1, 289.38, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3023, 242, '2025-06-28', 'North', 'Clothing', 2, 150.99, NULL, TRUE, 21, '2025-06-18 16:05:06'),
        (3024, 223, '2025-06-13', 'South', 'Home', 4, 312.35, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3025, 222, '2025-05-23', 'East', 'Electronics', 3, 325.0, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3026, 239, '2025-06-30', 'South', 'Clothing', 3, 105.74, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3027, 248, '2025-06-05', 'West', 'Clothing', 1, 334.86, 'Cash', FALSE, 21, '2025-06-18 16:05:06'),
        (3028, 244, '2025-05-26', 'South', 'Books', 2, 130.55, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3029, 234, '2025-06-14', 'West', 'Home', 2, 119.96, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3030, 218, '2025-05-12', 'North', 'Books', 2, 320.55, 'Credit Card', FALSE, 21, '2025-06-18 16:05:06'),
        (3031, 205, '2025-05-28', 'West', 'Books', 4, 94.8, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (3032, 201, '2025-05-30', 'South', 'Home', 5, 217.56, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3033, 218, '2025-06-03', 'East', 'Home', 1, 62.42, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3034, 240, '2025-05-18', 'West', 'Electronics', 3, 121.76, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (3035, 231, '2025-06-26', 'West', 'Electronics', 5, 343.56, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3036, 208, '2025-05-06', 'South', 'Books', 1, 369.65, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3037, 223, '2025-06-19', 'North', 'Clothing', 2, 263.79, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3038, 203, '2025-06-24', 'South', 'Books', 1, 496.0, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3039, 229, '2025-06-08', 'West', 'Clothing', 3, 329.98, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3040, 239, '2025-05-13', 'North', 'Home', 5, 58.54, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3041, 246, '2025-05-17', 'East', 'Home', 1, 232.51, 'Cash', TRUE, 21, '2025-06-18 16:05:06'),
        (3042, 214, '2025-06-25', 'South', 'Home', 5, 493.68, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3043, 224, '2025-06-16', 'East', 'Books', 3, 47.59, 'Bank Transfer', TRUE, 21, '2025-06-18 16:05:06'),
        (3044, 242, '2025-05-07', 'East', 'Electronics', 4, 118.25, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (3045, 222, '2025-05-11', 'West', 'Home', 3, 72.21, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3046, 222, '2025-05-17', 'North', 'Electronics', 3, 405.48, 'PayPal', TRUE, 21, '2025-06-18 16:05:06'),
        (3047, 239, '2025-05-07', 'North', 'Clothing', 5, 123.44, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06'),
        (3048, 236, '2025-05-09', 'South', 'Books', 5, 422.19, 'PayPal', FALSE, 21, '2025-06-18 16:05:06'),
        (3049, 244, '2025-05-19', 'West', 'Electronics', 1, 237.75, 'Credit Card', TRUE, 21, '2025-06-18 16:05:06'),
        (3050, 250, '2025-05-29', 'West', 'Books', 4, 233.76, 'Bank Transfer', FALSE, 21, '2025-06-18 16:05:06');
        """)
