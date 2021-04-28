# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [2.1.0b3] - 2021-04-28
### Fixed
- #281 SQLServer: Fix soda analyze  
- #159 SQLServer: Limit (TOP) works in queries
- #261 Metrics: scan command fails when a date validation is added
- #267 Metrics: scan command fails when valid_format is added to  
- #283 Snowflake: role and other parameters can be configured in warehouse.yml

## [2.1.0b2] - 2021-04-20
### Fixed
- #277 Fixed metric_groups not calculating all relevant metrics
- #235 Frequent values are calculated for al metric types
- #262 Fix Soda CLI return code
- #241 `soda create bigquery` command doesn't throw error anymore
- #257 Added scopes to Bigquery credentials
- Default scan time is set to UTC

### Changed
- Soda docs are now in a separate [repository](https://github.com/sodadata/docs)

## [2.1.0b1] - 2021-04-08
### Added
- Separated code into different modules, see [RFC](https://github.com/sodadata/soda-sql/discussions/209)

## [2.0.0b27] - 2021-04-08
### Added
- Documentation updates
### Fixed
- Warehouse fixture cleanup 

## [2.0.0b26] - 2021-03-31
### Changed
- Update dependencies to their latest versions
- Fix JSON serialization issues with decimals
- Refactor the way how errors are shown during a scan
- Add support for Metric Groups
- Add experimental support for SQL Metrics which produce `failed_rows`
- Add experimental support for dataset sampling (Soda Cloud)

## [2.0.0b25] - 2021-03-17
### Improved
- new parameter `catalog` added in Athena warehouse configuration

### Changed
- Hive support dependencies have been included in the release package

## [2.0.0b24] - 2021-03-16
### Changed
- Athena complex row type fix

## [2.0.0b23] - 2021-03-16
### Changed
- Fixed metadata bug

## [2.0.0b22] - 2021-03-15
### Changed
- Fixed bug in serialization of group values
- Added Hive dialect (Thanks, @ilhamikalkan !)
- In scan, skipping columns with unknown data types
- Improved analyzer should avoid full scan (Thanks, @mmigdiso !)
- Fixed valid_values configuration

## [2.0.0b21] - 2021-03-08

### Changed
- Improved error handling for Postgres/Redshift
- Adaptations in `.editorconfig` to align with PEP-8
- Documentation improvements and additions
- Managed scan support in scan launching flow

## [2.0.0b20] - 2021-03-05

### Changed
- Error handling of failed scans improvements
- Fixed histogram query filter bug. Thanks mmigdiso for the contribution!
- Added build badge to README.md

## [2.0.0b19] - 2021-03-04
###
- `2.0.0b18` was a broken release missing some files. This releases fixes that.
  It does not introduce any new features.

## [2.0.0b18] - 2021-03-04
### Changed
- Fixed bug in analyze command in BigQuery for STRUCT & ARRAY columns

## [2.0.0b17] - 2021-03-04
### Added
- Added support for Python 3.9 (Big tnx to the Snowflake connector update!)
### Changed
- Switched from yaml FullLoader to SafeLoader as per suggestion of 418sec.  Tnx!
- Fixed Decimal jsonnable bug when serializing test results
- Improved logging

## [2.0.0b16] - 2021-03-01
### Changed
- Initial SQLServer dialect implementation (experimental), contributed by Eric.  Tnx!
- Scan logging improvements
- Fixed BigQuery connection property docs
- Fixed json Decimal serialization bug when sending test results to Soda cloud
- Fixed validity bug (min/max)

## [2.0.0b15] - 2021-02-24
### Changed
- Add support for role assumption in Athena and Redshift
- Add support for detection of connectivity and authentication issues
- Improvements in cross-dialect quote handling

## [2.0.0b14] - 2021-02-23
### Changed
- Added create connection args / kwargs
- Fixed missing values in the soda scan start request

## [2.0.0b13] - 2021-02-22
### Changed
- Fixed Athena schema property #110 111 : Tnx Lieven!
- Improved Airflow & filtering docs
- Improved mins / maxs for text and time columns

## [2.0.0b12] - 2021-02-16
### Changed
- Renamed CLI command init to analyze!
- (Internal refactoring) Extracted dataset analyzer

## [2.0.0b11] - 2021-02-16
### Changed
- Added missing table metadata queries for athena and bigquery (#97)

## [2.0.0b10] - 2021-02-11
### Changed
- Internal changes (token authorization)

## [2.0.0b9] - 2021-02-10
### Changed
- Internal changes (scan builder, authentication and API changes)

## [2.0.0b8] - 2021-02-08
### Changed
- Fixed init files
- Fixed scan logs

## [2.0.0b7] - 2021-02-06
### Changed
- Moved the SQL metrics inside the scan YAML files, with option to specify the SQL inline or refer to a file.
### Added
- Added column level SQL metrics
- Added documentation on SQL metric variables

## [2.0.0b6] - 2021-02-05
### Changed
- Upgrade library dependencies (dev-requirements.in) to their latest patch
- In scan.yml files, extracted metric categories from `metrics` into a separate `metric_groups` element

## [2.0.0b5] - 2021-02-01
### Changed
- Fixed Snowflake CLI issue

## [2.0.0b4] - 2021-02-01
### Changed
- Packaging issue solved which blocked CLI bootstrapping

## [2.0.0b3] - 2021-01-31
### Added
- Support for Snowflake
- Support for AWS Redshift
- Support for AWS Athena
- Support for GCP BigQuery
- Anonymous tests
### Changed
- Improved [docs on tests](https://docs.soda.io/soda-sql/documentation/tests.html)

## [2.0.0b2] - 2021-01-22
### Added
- Support for AWS PostgreSQL
### Changed
- Published [docs online](https://docs.soda.io/soda-sql/)

## [2.0.0b1] - 2021-01-15
Initial release
