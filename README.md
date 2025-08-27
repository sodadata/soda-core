# Soda Core

This page is the central starting point for developers that want to work on this codebase. All engineering 
workflows you need as a developer should be explained in this page or provide you with links.

### Engineering workflow scripts

As much as possible, the engineering workflows should be supported with bash-scripts that are located 
in the `scripts` folder.

### Creating the development virtual environment

Run [`scripts/recreate_venv.sh`](scripts/recreate_venv.sh) to create a virtual environment in the `.venv` folder

To activate the virtual environment on your command line, run `. .venv/bin/activate`

To deactivate your virtual environment, run `deactivate`

### Starting & stopping the postgres docker container

[`scripts/start_postgres.sh`](scripts/start_postgres.sh)

To run the test suite on your local development environment, you need a postgres container. 
The above command will block the command line so that you can stop the postgres server using CTRL+C.

### Creating a new release

Every time a commit is done to `main` on the `v4` branch, a release is triggered automatically to pypi.dev.sodadata.io 

### Running the test suite in PyCharm

In your IDE, set up 

Ensure [your local postgres db is running (see below)](#starting-the-postgres-docker-container).

### Running the test suite in VSCode