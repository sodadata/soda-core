# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
