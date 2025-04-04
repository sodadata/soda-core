# Soda Cloud

### Example of a typical Soda Cloud configuration file

```yaml
soda_cloud:
  host: cloud.soda.io
  api_key_id: ${env.SODA_CLOUD_API_KEY_ID}
  api_key_secret: ${env.SODA_CLOUD_API_KEY_SECRET}
```

For more advanced use cases, you can also configure keys: `token`, `port` and `scheme`.  

### Specifying credentials in Soda Cloud YAML files

As you can see in the example, credentials can be specified in a Soda Cloud YAML configuration 
file via environment variables or also via variables that are provided via the CLI or Python API.

In general, the text `${env.SOME_ENV_VAR}` in a Soda Cloud YAML configuration file will be replaced 
with the value in environment variable `SOME_ENV_VAR`   

Similarly, the text `${var.SOME_ENV_VAR}` in a Soda Cloud YAML configuration file will be replaced 
with the value provided as variable `SOME_ENV_VAR` in the CLI or Python API

Refer to the [CLI docs on specifying variables](cli.md#specifying-variables-in-the-cli)
Refer to the [Soda Python API docs on specifying variables](python_api.md#variables-in-a-contract-verification)
