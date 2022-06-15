# Developer documentation

This is a guide providing pointers to all tasks related to the development of Soda Core and Soda Checks Language.

## Get started

To contribute, fork the sodadata/soda-core GitHub repo. 

## Folder structure

```
soda-core project root folder
├── soda                  # Root for all Python packages
│   ├── core              # Root for the soda-core package
│   │   ├── soda          # Python source code for the soda-core package 
│   │   └── tests         # Test suite code and artefacts for soda-core package
│   ├── scientific        # Root for the scientific package 
│   ├── postgres          # Root for the soda-core-postgres package 
│   ├── snowflake         # Root for the soda-core-snowflake package
│   └── ...               # Root for the other data source packages
├── scripts               # Scripts for developer workflows
├── dev-requirements.in   # Test suite dependencies
├── dev-requirements.txt  # Generated test suite dependencies
├── requirements.txt      # Generated test suite dependencies
├── LICENSE               # Apache 2.0 license
└── README.md             # Pointer to the online docs for end users and github home page
```

## Requirements

* Python 3.8 or greater. 

To check the version of your existing Python install, use:
```
> python --version
Python 3.8.12
>
```

Although not required, we recommend using [pyenv](https://github.com/pyenv/pyenv) to more easily manage multiple Python
versions.

## Creat3 a virtual environment

This repo includes a convenient script to create a virtual environment.

The `scripts/recreate_venv.sh` script installs the dependencies in your virtual environment.  Review the contents of the file
as inspiration if you want to manage the virtual environment yourself.

```
> scripts/recreate_venv.sh
Requirement already satisfied: pip in ./.venv/lib/python3.8/site-packages (21.1.1)
Collecting pip
  Using cached pip-21.3.1-py3-none-any.whl (1.7 MB)
Installing collected packages: pip
  Attempting uninstall: pip

...lots of output and downloading...

Successfully installed Jinja2-2.11.3 MarkupSafe-2.0.1 cffi-1.15.0 click-8.0.3 cryptography-3.3.2 pycparser-2.21 ruamel.yaml-0.17.17 ruamel.yaml.clib-0.2.6 soda-sql-core-v3-3.0.0-prerelease-1
>
```



## Activate a virtual environment

```
source .venv/bin/activate
```
To deactivate the virtual environment, use the following command:

```
deactivate
```

## Run tests

### Postgres test database as a docker container

Running the test suite requires a Postgres DB running on localhost having a user `sodasql`
without a password, database `sodasql` with a `public` schema.  Simplest way to get one
up and running is
```shell
> scripts/start_postgres_container.sh
```
The above command will launch a postgres needed for running the test suite as a docker container.

### Running the basic test suite
Then run the test suite using
```shell
> scripts/run_tests.sh
```

Before pushing commits or asking to review a pull request, we ask that you verify successful execution of
the following test suite on your machine.

### Testing cross cutting concerns

There are a couple of cross cutting concerns that need to be tested over a variety of functional
test scenarios.  To do this, we introduce environment variables that if set, activate the cross
cutting feature while executing the full test suite.

* `export WESTMALLE=LEKKER` : activates soda cloud connection
* `export CHIMAY=YUMMIE` : activates local storage of files
* `export ROCHEFORT=HMMM` : activates notifications

## Testing with Tox

- We use [Tox](https://tox.wiki/en/latest/) to run tests and `.env` file to set up a data source to run them with.
- Create a `.env` file and fill it with relevant data source information (see `.env.example` for inspiration).
- Run `tox`.
