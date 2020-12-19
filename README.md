# sodasql

## Running Tests

To run all unit tests simply execute the following command:

```
$ tox
```

Tox will start a Postgres database as a docker image and stop it after the tests are finished.

You may also pass extra parameters. This shows everything written to the standard output (even if tests don't fail):

```
$ tox -- -s
```

This changes the log level:

```
$ tox -- --log-cli-level=DEBUG
```

To generate HTML test reports, execute the following command:

```
$ tox -e html-test-reports
```

Reports will be available in the directory `./reports/tests/`.
