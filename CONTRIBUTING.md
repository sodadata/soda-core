# Developer documentation

This is a guide providing pointers to all tasks related to the development of Soda Checks.

## Cloning the repo

```
> git clone git@github.com:sodadata/soda-core.git
Cloning into 'soda-core'...
remote: Enumerating objects: 5319, done.
remote: Counting objects: 100% (5319/5319), done.
remote: Compressing objects: 100% (2841/2841), done.
remote: Total 5319 (delta 3358), reused 4187 (delta 2253), pack-reused 0
Receiving objects: 100% (5319/5319), 1.44 MiB | 3.69 MiB/s, done.
Resolving deltas: 100% (3358/3358), done.
>
```

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

You'll need Python 3.8+
```
> python --version
Python 3.8.12
>
```

In case you need to go and get it, see [Python downloads](https://www.python.org/downloads/)

Although not required, we recommend to use [pyenv](https://github.com/pyenv/pyenv) to more easily manage multiple Python
versions.

## Creating the virtual environment

There is a convenience script `scripts/recreate_venv.sh` to create the virtual environment:

```
> scripts/recreate_venv.sh
Requirement already satisfied: pip in ./.venv/lib/python3.8/site-packages (21.1.1)
Collecting pip
  Using cached pip-21.3.1-py3-none-any.whl (1.7 MB)
Installing collected packages: pip
  Attempting uninstall: pip

...loads of output and downloading going on here...

Successfully installed Jinja2-2.11.3 MarkupSafe-2.0.1 cffi-1.15.0 click-8.0.3 cryptography-3.3.2 pycparser-2.21 ruamel.yaml-0.17.17 ruamel.yaml.clib-0.2.6 soda-sql-core-v3-3.0.0-prerelease-1
>
```

The `scripts/recreate_venv.sh` script will install the dependencies in your virtual environment.  See the contents
as inspiration if you want to manage the virtual environment yourself.

## Activating the virtual environment

```
> source .venv/bin/activate
(.venv) >
```

## Running tests

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
