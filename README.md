<p align="center"><img src="https://github.com/sodadata/docs/blob/main/assets/images/soda-sql-logo.png" alt="Soda logo" width="300" /></p>

<p align="center"><b>Data testing, monitoring, and profiling for SQL-accessible data.</b></p>

<p align="center">
  <a href="https://github.com/sodadata/soda-sql/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202-blue.svg" alt="License: Apache 2.0"></a>
  <a href="#contributors"><img src="https://img.shields.io/badge/all_contributors-21-orange.svg?style=flat-square)"></a>
  <a href="https://join.slack.com/t/soda-community/shared_invite/zt-m77gajo1-nXJF7JtbbRht2zwaiLb9pg"><img alt="Slack" src="https://img.shields.io/badge/chat-slack-green.svg"></a>
  <a href="https://pypi.org/project/soda-sql/"><img alt="Pypi Soda SQL" src="https://img.shields.io/badge/pypi-soda%20sql-green.svg"></a>
  <a href="https://github.com/sodadata/soda-sql/actions/workflows/build.yml"><img alt="Build soda-sql" src="https://github.com/sodadata/soda-sql/actions/workflows/build.yml/badge.svg"></a>
</p>
 <br />
 <br />

<p>&#10004;  <a href="https://docs.soda.io/soda-sql/installation.html">Install</a> from the command-line<br /></p>
<p>&#10004;  Access comprehensive <a href="https://docs.soda.io/soda-sql/overview.html">documentation</a><br /></p>
<p>&#10004;  Compatible with Snowflake, Amazon Redshift, BigQuery, <a href="https://docs.soda.io/soda-sql/installation.html#compatibility">and more</a><br /></p>
<p>&#10004;  <a href="https://docs.soda.io/soda-sql/tests.html">Write tests</a> in a YAML file<br /></p>
<p>&#10004;  <a href="https://docs.soda.io/soda-sql/programmatic_scan.html">Run programmatic scans</a> to test data quality<br /></p>
<br />

**Got 5 minutes?** <a href="https://docs.soda.io/soda-sql/interactive-demo.html" target="_blank">Try the interactive demo!</a><br />
<br />

#### Example scan YAML file
```yaml
table_name: breakdowns
metrics:
  - row_count
  - missing_count
  - missing_percentage
...
# Validates that a table has rows
tests:
  - row_count > 0

# Tests that numbers in the column are entered in a valid format as whole numbers
columns:
  incident_number:
    valid_format: number_whole
    tests:
      - invalid_percentage == 0

# Tests that no values in the column are missing
  school_year:
    tests:
      - missing_count == 0

# Tests for duplicates in a column
  bus_no:
    tests:
      - duplicate_count == 0

# Compares row count between datasets
sql_metric: 
  sql: |
    SELECT COUNT(*) as other_row_count
    FROM other_table
  tests:
    - row_count == other_row_count
```

## Play
* <a href="https://docs.soda.io/soda-sql/interactive-demo.html">Interactive Soda SQL demo</a>
* <a href="https://github.com/sodadata/tutorial-demo-project" target="_blank">Soda SQL playground repo</a>

## Install
* <a href="https://docs.soda.io/soda-sql/installation.html">Install Soda SQL</a>
* <a href="https://docs.soda.io/soda-sql/5_min_tutorial.html">Quick start for Soda SQL</a>

## Collaborate
* <a href="https://community.soda.io/slack" target="_blank">Join us on Slack</a>
* <a href="https://github.com/sodadata/soda-sql/blob/main/CONTRIBUTING.md" target="_blank">Help develop Soda SQL</a>


## Contributors ✨

Thanks goes to these wonderful people! ([emoji key](https://allcontributors.org/docs/en/emoji-key))

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://www.vijaykiran.com"><img src="https://avatars.githubusercontent.com/u/23506?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Vijay Kiran</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=vijaykiran" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/abhishek-khare"><img src="https://avatars.githubusercontent.com/u/44169877?v=4?s=100" width="100px;" alt=""/><br /><sub><b>abhishek khare</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=abhishek-khare" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/jcshoekstra"><img src="https://avatars.githubusercontent.com/u/6941860?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jelte Hoekstra</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=jcshoekstra" title="Code">💻</a> <a href="https://github.com/sodadata/soda-sql/commits?author=jcshoekstra" title="Documentation">📖</a></td>
    <td align="center"><a href="https://github.com/JCZuurmond"><img src="https://avatars.githubusercontent.com/u/5946784?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Cor</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=JCZuurmond" title="Code">💻</a> <a href="https://github.com/sodadata/soda-sql/commits?author=JCZuurmond" title="Documentation">📖</a></td>
    <td align="center"><a href="https://aleksic.dev"><img src="https://avatars.githubusercontent.com/u/50055?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Milan Aleksić</b></sub></a><br /><a href="#infra-milanaleksic" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    <td align="center"><a href="http://www.fakir.dev"><img src="https://avatars.githubusercontent.com/u/5069674?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Ayoub Fakir</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=fakirAyoub" title="Code">💻</a></td>
    <td align="center"><a href="https://tonkonozhenko.com"><img src="https://avatars.githubusercontent.com/u/1307646?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Alex Tonkonozhenko</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=Tonkonozhenko" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://toddy86.github.io/blog/"><img src="https://avatars.githubusercontent.com/u/10559757?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Todd de Quincey</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=toddy86" title="Code">💻</a></td>
    <td align="center"><a href="https://antoninjsn.netlify.app"><img src="https://avatars.githubusercontent.com/u/18756890?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Antonin Jousson</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=Antoninj" title="Code">💻</a></td>
    <td align="center"><a href="http://www.omeyocan.be"><img src="https://avatars.githubusercontent.com/u/153805?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jonas</b></sub></a><br /><a href="#infra-jmarien" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
    <td align="center"><a href="http://www.webunit.be"><img src="https://avatars.githubusercontent.com/u/1439383?v=4?s=100" width="100px;" alt=""/><br /><sub><b>cwouter</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=cwouter" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/janet-can"><img src="https://avatars.githubusercontent.com/u/63879030?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Janet R</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=janet-can" title="Documentation">📖</a></td>
    <td align="center"><a href="http://www.bastienboutonnet.com"><img src="https://avatars.githubusercontent.com/u/4304794?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Bastien Boutonnet</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=bastienboutonnet" title="Code">💻</a></td>
    <td align="center"><a href="https://twitter.com/tombaeyens"><img src="https://avatars.githubusercontent.com/u/944245?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Tom Baeyens</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=tombaeyens" title="Code">💻</a></td>
  </tr>
  <tr>
    <td align="center"><a href="https://github.com/AlessandroLollo"><img src="https://avatars.githubusercontent.com/u/1206243?v=4?s=100" width="100px;" alt=""/><br /><sub><b>AlessandroLollo</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=AlessandroLollo" title="Code">💻</a></td>
    <td align="center"><a href="https://www.linkedin.com/in/migdisoglu/"><img src="https://avatars.githubusercontent.com/u/4024345?v=4?s=100" width="100px;" alt=""/><br /><sub><b>mmigdiso</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=mmigdiso" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/ericmuijs"><img src="https://avatars.githubusercontent.com/u/17954084?v=4?s=100" width="100px;" alt=""/><br /><sub><b>ericmuijs</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=ericmuijs" title="Code">💻</a></td>
    <td align="center"><a href="https://www.elaidata.com"><img src="https://avatars.githubusercontent.com/u/1313689?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Lieven Govaerts</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=lgov" title="Code">💻</a></td>
    <td align="center"><a href="http://justsomegeek.com"><img src="https://avatars.githubusercontent.com/u/582933?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Milan Lukac</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=m1n0" title="Code">💻</a></td>
    <td align="center"><a href="https://github.com/sebastianvillarroel"><img src="https://avatars.githubusercontent.com/u/16538437?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Sebastián Villarroel</b></sub></a><br /><a href="https://github.com/sodadata/soda-sql/commits?author=sebastianvillarroel" title="Code">💻</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind are welcome!

## Open Telemetry Tracking

Soda-sql collects statistical usage and performance information via the [Open Telemetry framework](https://opentelemetry.io) to help the Soda Core developers team proactively track performance issues and understand how users interact with the tool.
The information is strictly limited to usage and performance and does not contain Personal Identifying Information. It will be used for internal purposes only. Soda will keep the data in its raw form for a maximum of 5 years. If some information needs to be kept for longer, it will be done in aggregated form only.

Users can find more information about the tracked information, and opt-out of tracking by consulting the [reference section of docs.soda.io](https://docs.soda.io/soda-sql/global-configuration.html)
