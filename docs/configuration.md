# Configure Soda Core 

After you [install Soda Core](/docs/installation.md), you must create a `configuration.yml` to provide details for Soda Core to connect your data source (except Apache Spark DataFrames, which does not use a configuration YAML file).

Alternatively, you can provide data source connection configurations in the context of a [programmatic scan](/docs/programmatic.md), if you wish.

[Configuration instructions](#configuration-instructions)<br />
[Provide credentials as system variables](#provide-credentials-as-system-variables)<br />
[Disable failed rows samples for specific columns](#disable-failed-rows-samples-for-specific-columns)<br />
[Disable failed rows samples for individual checks](#disable-failed-row-samples-for-individual-checks)<br />
<br />

## Configuration instructions

1. Soda Core connects with Spark DataFrames in a unique way, using programmtic scans.
    * If you are using Spark DataFrames, follow the configuration details in [Connect to Spark DataFrames](https://docs.soda.io/soda/connect-spark.html#connect-to-spark-dataframes).
    * If you are *not* using Spark DataFrames, continue to step 2.
2. Create a `configuration.yml` file. This file stores connection details for your data sources. Use the data source-specific connection configurations listed below to copy+paste the connection syntax into your file, then adjust the values to correspond with your data source's details. You can use [system variables](#provide-credentials-as-system-variables) to pass sensitive values, if you wish. Access connection details in [Connect a data source](https://docs.soda.io/soda/connect-athena.html) section of Soda documentation; see below for MS Fabric connection config as it is only supported in Soda Core.
3. Save the `configuration.yml` file, then create another new YAML file named `checks.yml`. 
4. A Soda Check is a test that Soda Core performs when it scans a dataset in your data source. The checks YAML file stores the Soda Checks you write using [SodaCL](https://docs.soda.io/soda-cl/soda-cl-overview.html). Copy+paste the following basic check syntax in your file, then adjust the value for `dataset_name` to correspond with the name of one of the datasets in your data source.
    ```yaml
    checks for dataset_name:
      - row_count > 0
    ```
5. Save the changes to the `checks.yml` file.
6. Next: [run a scan](/docs/scan-core.md) of the data in your data source.

#### MS Fabric connection configuration

To your `configuration.yml` file, add the following.
```yaml
data_source my_data_source_name: 
  type: fabric
  host: xxx
  database: xxx
  schema: xxx
  driver: ODBC Driver 18 for SQL Server
  client_id: xxx
  client_secret: xxx
  encrypt: True
  authentication: xxx
```

## Provide credentials as system variables

If you wish, you can provide data source login credentials or any of the properties in the configuration YAML file as system variables instead of storing the values directly in the file. System variables persist only for as long as you have the terminal session open in which you created the variable. For a longer-term solution, consider using permanent environment variables stored in your `~/.bash_profile` or `~/.zprofile` files.

1. From your command-line interface, set a system variable to store the value of a property that the configuration YAML file uses. For example, you can use the following command to define a system variable for your password.
    ```shell
    export POSTGRES_PASSWORD=1234
    ```
2. Test that the system retrieves the value that you set by running an `echo` command.
    ```shell
    echo $POSTGRES_PASSWORD
    ```
3. In the configuration YAML file, set the value of the property to reference the environment variable, as in the following example.
    ```yaml
    data_source my_database_name:
      type: postgres
      connection:
        host: soda-temp-demo
        port: '5432'
        username: sodademo
        password: ${POSTGRES_PASSWORD}
        database: postgres
        schema: public
    ```
4. Save the configuration YAML file, then run a scan to confirm that Soda Core connects to your data source without issue.
    ```shell
    soda scan -d your_datasource -c configuration.yml checks.yml
    ```

## Disable failed rows samples for specific columns

For checks which implicitly or explcitly collect [failed rows samples](https://docs.soda.io/soda-cl/failed-rows-checks.html#about-failed-row-samples), you can add a configuration to your configuration YAML file to prevent Soda from collecting failed rows samples from specific columns that contain sensitive data. 

Refer to [Disable failed rows sampling for specific columns](https://docs.soda.io/soda-cl/failed-rows-checks.html#disable-failed-rows-sampling-for-specific-columns).


## Disable failed row samples for individual checks

For checks which implicitly or explcitly collect failed rows samples, you can set the `samples limit` to `0` to prevent Soda from collecting failed rows samples (and sending the samples to Soda Cloud, if you have connected it to Soda Core) for an individual check, as in the following example.

```yaml
checks for dim_customer:
  - missing_percent(email_address) < 50:
      samples limit: 0
```
<br />