from sodasql.scan.scan_builder import ScanBuilder

warehouse_yml = '/Users/ilhami.kalkan/projects/myprojects/soda_sql_tutorial/warehouse_hive.yml'
scan_yml = '/Users/ilhami.kalkan/projects/myprojects/soda_sql_tutorial/tables/pokes.yml'

scan_builder = ScanBuilder()
scan_builder.warehouse_yml_file = warehouse_yml
scan_builder.scan_yml_file = scan_yml
scan = scan_builder.build()
scan_result = scan.execute()
if scan_result.has_failures():
    failures = scan_result.failures_count()
    raise ValueError(f"Soda Scan found {failures} errors in your data!")
