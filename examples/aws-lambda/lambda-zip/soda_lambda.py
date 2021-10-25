from sodasql.scan.scan_builder import ScanBuilder
from sodasql.__version__ import SODA_SQL_VERSION


def lambda_handler(event, context):
    print(f'Lambda Handler: Soda SQL Version: {SODA_SQL_VERSION}')
    scan_builder = ScanBuilder()
    scan_builder.warehouse_yml_dict = {
        'name': 'lambda-demo',
        'connection': {
            'type': 'postgres',
            'host': 'env_var(POSTGRES_URL)',
            'port': '5432',
            'username': 'env_var(POSTGRES_USERNAME)',
            'password': 'env_var(POSTGRES_PASSWORD)',
            'database': 'postgres',
            'schema': 'public'
        },
        'soda_account': {
            'host': 'cloud.soda.io',
            'api_key_id': 'env_var(API_PUBLIC)',
            'api_key_secret': 'env_var(API_PRIVATE)',
        }
    }

    scan_builder.scan_yml_file = 'product.yml'
    scan = scan_builder.build()
    scan_result = scan.execute()

    print("Finished: Soda Scan")
    print(scan_result.to_dict())
