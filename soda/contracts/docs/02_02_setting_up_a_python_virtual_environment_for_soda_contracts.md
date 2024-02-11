# Setting up a Soda contracts virtual environment

> Alternative: As an alternative to setting up your own virtual environment, consider using 
> a Soda docker container.  That will have a tested combination of all the library dependencies. 

> Prerequisites:
> * Linux or Mac
> * You have installed Python 3.8 or greater.
> * You have installed Pip 21.0 or greater.

This tutorial references a MacOS environment.

```shell
mkdir soda_contract
cd soda_contract
# Best practice dictates that you install the Soda using a virtual environment. In your command-line interface, create a virtual environment in the .venv directory, then activate the environment.
python3 -m venv .venv
source .venv/bin/activate
# Execute the following command to install the Soda package for PostgreSQL in your virtual environment. The example data is in a PostgreSQL data source, but there are 15+ data sources with which you can connect your own data beyond this tutorial.
pip install -i https://pypi.cloud.soda.io soda-postgres
pip install -i https://pypi.cloud.soda.io soda-core-contracts
# Validate the installation.
soda --help
```

To exit the virtual environment when you are done with this tutorial, use the command deactivate.
