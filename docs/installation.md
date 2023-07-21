# Install Soda Core 

**Soda Core** is a command-line interface (CLI) tool that enables you to scan the data in your data source to surface invalid, missing, or unexpected data. 

Alternatively, you can use the Soda Core Python library to programmatically execute scans; see [Define programmatic scans using Python](/docs/programmatic.md).
<br />

[Compatibility](#compatibility)<br />
[Requirements](#requirements)<br />
[Install on MacOS, Linux](#install-on-macos-linux)<br />
[Install on Windows](#install-on-windows)<br />
[Install using Docker](#install-using-docker)<br />
[Upgrade](#upgrade)<br />
[Install Soda Core Scientific](#install-soda-core-scientific)<br />

## Compatibility

Use Soda Core to scan a variety of data sources.<br />

<table>
  <tr>
    <td>Amazon Athena<br /> Amazon Redshift<br />  Apache Spark DataFrames<sup>1</sup><br /> Apache Spark for Databricks SQL<br /> Azure Synapse (Experimental)<br />ClickHouse (Experimental)<br />   Dask and Pandas (Experimental)<sup>1</sup><br /> Denodo (Experimental)<br />Dremio <br />DuckDB (Experimental)<br /> GCP Big Query</td>
    <td>IBM DB2<br /> Local file using Dask<sup>1</sup><br />MS SQL Server<br /> MySQL<br > OracleDB<br /> PostgreSQL<br /> Snowflake<br /> Teradata (Experimental) <br />Trino<br /> Vertica (Experimental)</td>
  </tr>
</table>
<sup>1</sup> For use with programmatic Soda scans, only.

## Requirements

To use Soda Core, you must have installed the following on your system.

* Python 3.8 or greater. To check your existing version, use the CLI command: `python --version` or `python3 --version` 
If you have not already installed Python, consider using <a href="https://github.com/pyenv/pyenv/wiki" target="_blank">pyenv</a> to manage multiple versions of Python in your environment.
* Pip 21.0 or greater. To check your existing version, use the CLI command: `pip --version`

## Install on MacOS, Linux

1. Best practice dictates that you install the Soda Core CLI using a virtual environment. In your command-line interface tool, create a virtual environment in the `.venv` directory using the commands below. Depending on your version of Python, you may need to replace `python` with `python3` in the first command.
    ```shell
    python -m venv .venv
    source .venv/bin/activate
    ```
2. Upgrade pip inside your new virtual environment.
    ```shell
    pip install --upgrade pip
    ```
3. Execute the following command, replacing `soda-core-postgres` with the install package that matches the type of data source you use to store data.
    ```shell
    pip install soda-core-postgres
    ```

| Data source | Install package | 
| ----------- | --------------- | 
| Amazon Athena | `soda-core-athena` |
| Amazon Redshift | `soda-core-redshift` | 
| Apache Spark DataFrames <br /> (For use with [programmatic Soda scans]({% link soda-core/programmatic.md %}), only.) | `soda-core-spark-df` |
| Azure Synapse (Experimental) | `soda-core-sqlserver` |
| ClickHouse (Experimental) | `soda-core-mysql` |
| Dask and Pandas (Experimental)  | `soda-core-pandas-dask` |
| Databricks  | `soda-core-spark[databricks]` |
| Denodo (Experimental) | `soda-core-denodo` |
| Dremio | `soda-core-dremio` | 
| DuckDB (Experimental)  | `soda-core-duckdb` |
| GCP Big Query | `soda-core-bigquery` | 
| IBM DB2 | `soda-core-db2` |
| Local file | Use Dask. |
| MS SQL Server | `soda-core-sqlserver` |
| MySQL | `soda-core-mysql` |
| OracleDB | `soda-core-oracle` |
| PostgreSQL | `soda-core-postgres` |
| Snowflake | `soda-core-snowflake` | 
| Teradata | `soda-core-teradata` |
| Trino | `soda-core-trino` |
| Vertica (Experimental) | `soda-core-vertica` |


To deactivate the virtual environment, use the following command:
```shell
deactivate
```

## Install on Windows

1. Best practice dictates that you install the Soda Core CLI using a virtual environment. In your command-line interface tool, create a virtual environment in the `.venv` directory using the commands below. Depending on your version of Python, you may need to replace `python` with `python3` in the first command. Reference the <a href="https://virtualenv.pypa.io/en/legacy/userguide.html#activate-script" target="_blank">virtualenv documentation</a> for activating a Windows script.
    ```shell
    python -m venv .venv
    .venv\Scripts\activate
    ```
2. Upgrade pip inside your new virtual environment.
    ```shell
    pip install --upgrade pip
    ```
3. Execute the following command, replacing `soda-core-postgres` with the install package that matches the type of data source you use to store data.
    ```shell
    pip install soda-core-postgres
    ```

| Data source | Install package | 
| ----------- | --------------- | 
| Amazon Athena | `soda-core-athena` |
| Amazon Redshift | `soda-core-redshift` | 
| Apache Spark DataFrame <br /> (For use with [programmatic Soda scans]({% link soda-core/programmatic.md %}), only.) | `soda-core-spark-df` |
| Azure Synapse (Experimental) | `soda-core-sqlserver` |
| ClickHouse (Experimental) | `soda-core-mysql` |
| Dask and Pandas (Experimental)  | `soda-core-pandas-dask` |
| Databricks  | `soda-core-spark[databricks]` |
| Denodo (Experimental) | `soda-core-denodo` |
| Dremio | `soda-core-dremio` | 
| DuckDB (Experimental) | `soda-core-duckdb` |
| GCP Big Query | `soda-core-bigquery` | 
| IBM DB2 | `soda-core-db2` |
| MS SQL Server | `soda-core-sqlserver` |
| MySQL | `soda-core-mysql` |
| OracleDB | `soda-core-oracle` |
| PostgreSQL | `soda-core-postgres` |
| Snowflake | `soda-core-snowflake` | 
| Teradata | `soda-core-teradata` |
| Trino | `soda-core-trino` |
| Vertica (Experimental) | `soda-core-vertica` |

To deactivate the virtual environment, use the following command:
```shell
deactivate
```

Reference the <a href="https://virtualenv.pypa.io/en/legacy/userguide.html#activate-script" target="_blank">virtualenv documentation</a> for activating a Windows script.

## Install using Docker

Use <a href="https://hub.docker.com/repository/docker/sodadata/soda-core" target="_blank">Soda's Docker image</a> in which Soda Core Scientific is pre-installed.

1. If you have not already done so, <a href="https://docs.docker.com/get-docker/" target="_blank">install Docker</a> in your local environment. 
2. From Terminal, run the following command to pull the latest Soda Core's official Docker image.
    ```shell
    docker pull sodadata/soda-core
    ```
3. Verify the pull by running the following command.
    ```shell
    docker run sodadata/soda-core --help
    ```
    Output:
    ```shell
        Usage: soda [OPTIONS] COMMAND [ARGS]...

        Soda Core CLI version 3.0.xxx

        Options:
        --help  Show this message and exit.

        Commands:
        scan    runs a scan
        update-dro  updates a distribution reference file
        ```
When you run the Docker image on a non-Linux/amd64 platform, you may see the following warning from Docker, which you can ignore.
```shell
WARNING: The requested image's platform (linux/amd64) does not match the detected host platform (linux/arm64/v8) and no specific platform was requested
```

### Run a scan with Docker
When you are ready to run a Soda scan, use the following command to run the scan via the docker image. Replace the placeholder values with your own file paths and names.
```bash
docker run -v /path/to/your_soda_directory:/sodacl sodadata/soda-core scan -d your_data_source -c /sodacl/your_configuration.yml /sodacl/your_checks.yml
``` 

Optionally, you can specify the version of Soda Core to use to execute the scan. This may be useful when you do not wish to use the latest released version of Soda Core to run your scans. The example scan command below specifies Soda Core version 3.0.0.
```bash
docker run -v /path/to/your_soda_directory:/sodacl sodadata/soda-core:v3.0.0 scan -d your_data_source -c /sodacl/your_configuration.yml /sodacl/your_checks.yml
```

<details>
  <summary style="color:#00BC7E"> What does the scan command do? </summary>
  <ul>
    <li><code>docker run</code> ensures that the docker engine runs a specific image.</li>
    <li><code>-v</code> mounts your SodaCL files into the container. In other words, it makes the configuration.yml and checks.yml files in your local environment available to the docker container. The command example maps your local directory to <code>/sodacl</code> inside of the docker container. </li>
    <li><code>sodadata/soda-core</code> refers to the image that <code>docker run</code> must use.</li>
    <li><code>scan</code> instructs Soda Core to execute a scan of your data. </li>
    <li><code>-d</code> indicates the name of the data source to scan.</li>
    <li><code>-c</code> specifies the filepath and name of the configuration YAML file.</li>
  </ul>
</details>

<br />

#### Error: Mounts denied

If you encounter the following error, follow the procedure below.

```shell
docker: Error response from daemon: Mounts denied: 
The path /soda-core-test/files is not shared from the host and is not known to Docker.
You can configure shared paths from Docker -> Preferences... -> Resources -> File Sharing.
See https://docs.docker.com/desktop/mac for more info.
```

You need to give Docker permission to acccess your configuration.yml and checks.yml files in your environment. To do so:
  1. Access your Docker Dashboard, then select Preferences (gear symbol).
  2. Select Resources, then follow the <a href="https://docs.docker.com/desktop/mac/#file-sharing" target="_blank">Docker instructions</a> to add your Soda project directory -- the one you use to store your configuration.yml and checks.yml files -- to the list of directories that can be bind-mounted into Docker containers. 
  3. Click Apply & Restart, then repeat steps above.

<br />

#### Error: Configuration path does not exist

If you encounter the following error, double check the syntax of the scan command in step 4 above. 
* Be sure to prepend `/sodacl/` to both the congifuration.yml filepath and the checks.yml filepath. 
* Be sure to mount your files into the container by including the `-v` option.  For example, `-v /Users/MyName/soda_core_project:/sodacl`.

```shell
Soda Core 3.0.xxx
Configuration path 'configuration.yml' does not exist
Path "checks.yml" does not exist
Scan summary:
No checks found, 0 checks evaluated.
2 errors.
Oops! 2 errors. 0 failures. 0 warnings. 0 pass.
ERRORS:
Configuration path 'configuration.yml' does not exist
Path "checks.yml" does not exist
```

<br />


## Upgrade

To upgrade your existing Soda Core tool to the latest version, use the following command, replacing `soda-core-redshift` with the install package that matches the type of data source you are using.
```shell
pip install soda-core-redshift -U
```

## Install Soda Core Scientific

Install Soda Core Scientific to be able to use SodaCL [distribution checks](https://docs.soda.io/soda-cl/distribution.html) or [anomaly score checks](https://docs.soda.io/soda-cl/anomaly-score.html).

You have two installation options to choose from:
* [Install Soda Core Scientific in a virtual environment (Recommended)](#install-soda-core-scientific-in-a-virtual-environment-recommended)
* [Use Docker to run Soda Core with Soda Scientific](#install-using-docker)

### Install Soda Core Scientific in a virtual environment (Recommended)

1. Set up a virtual environment, as described above.
2. Install Soda Core in your new virtual environment, as described above.
3. Use the following command to install Soda Core Scientific.
    ```bash
    pip install soda-core-scientific
    ```

Note that installing the Soda Core Scientific package also installs several scientific dependencies. Reference the <a href="https://github.com/sodadata/soda-core/blob/main/soda/scientific/setup.py" target="_blank">soda-core-scientific setup file</a> in the public GitHub repository for details.



#### Error: Library not loaded

If you have defined an `anomaly score` check and you use an M1 MacOS machine, you may get a `Library not loaded: @rpath/libtbb.dylib` error. This is a known issue in the MacOS community and is caused by issues during the installation of the <a href="https://github.com/facebook/prophet" target="_blank">prophet library</a>. There currently are no official workarounds or releases to fix the problem, but the following adjustments may address the issue.

1. Install `soda-core-scientific` as per the [virtual environment installation instructions](#install-soda-core-scientific-in-a-virtual-environment-recommended) and activate the virtual environment.
2. Use the following command to navigate to the directory in which the `stan_model` of the `prophet` package is installed in your virtual environment.
    ```shell
    cd path_to_your_python_virtual_env/lib/pythonyour_version/site_packages/prophet/stan_model/
    ```
    For example, if you have created a python virtual environment in a `/venvs` directory in your home directory and you use Python 3.9, you would use the following command.
    ```shell
    cd ~/venvs/soda-core-prophet11/lib/python3.9/site-packages/prophet/stan_model/
    ```
3. Use the `ls` command to determine the version number of `cmndstan` that `prophet` installed. The `cmndstan` directory name includes the version number.
    ```shell
    ls
    cmdstan-2.26.1		prophet_model.bin
    ```
4. Add the `rpath` of the `tbb` library to your `prophet` installation using the following command.
    ```shell
    install_name_tool -add_rpath @executable_path/cmdstanyour_cmdstan_version/stan/lib/stan_math/lib/tbb prophet_model.bin
    ```
    With `cmdstan` version `2.26.1`, you would use the following command.
    ```bash
    install_name_tool -add_rpath @executable_path/cmdstan-2.26.1/stan/lib/stan_math/lib/tbb prophet_model.bin
    ```
