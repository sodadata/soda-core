By default, the output of a Soda SQL scan appears in your command-line interface. In the example below, Soda SQL executed three tests and all the tests passed. The `Exit code` is a process code: 0 indicates success with no test failures; a non-zero number indicates failures.

```shell
  | < 200 {}
  | 54 measurements computed
  | 3 tests executed
  | All is good. No tests failed.
  | Exiting with code 0
```

In the following example, some of the tests Soda SQL executed failed, as indicated by `Exiting with code 2`. The scan output indicates which tests failed and why, so that you can investigate and resolve the issues in the warehouse.

```shell
  | < 200 {}
  | 304 measurements computed
  | 8 tests executed
  | 2 of 8 tests failed:
  |   Test column(EMAIL) test(missing_count == 0) failed with metric values {"missing_count": 33}
  |   Test column(CREDIT_CARD_NUMBER) test(invalid_percentage == 0) failed with metric values {"invalid_percentage": 28.4}
  | Exiting with code 2 
```