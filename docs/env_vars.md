# Env vars

In `soda.yml` project configuration files, you can refer to 
environment variables with by using `env_var(SOME_ENV_VAR)` as the value.

You can set the environment variables manually.

But the `soda` commands offer a convenience to manage your local credentials 
as environment variables.  Each time the `soda` command will read a project 
configuration, it will also set environment variables specified in the project 
section in your `~/.soda/env_vars.yml`

This is a convenient way you keep your soda project configuration files 
free of any credentials

Example `~/.soda/env_vars.yml`
```yaml
my_project_postgres:
    SNOWFLAKE_USERNAME: someotherexampleusername
    SNOWFLAKE_PASSWORD: someotherexamplepassword

some_other_soda_project:
    POSTGRES_USERNAME: myexampleusername
    POSTGRES_PASSWORD: myexamplepassword
```

The `soda create` command will assist in creating and prepopulating the 
environment variables section in your `~/.soda/env_vars.yml`