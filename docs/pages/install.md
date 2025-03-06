# Install Soda Core in a Python virtual environment

### Requires

* Python 3.9.9+\
  Verify that you have a compatible Python version\
  `python --version` It should show 3.9.9+
* Docker or a postgres db
* A Soda Cloud account on https://dev.sodadata.io/

### Set up a project folder

Create an empty project root directory. Eg `ccli`

### Install the Soda CLI

Open a command prompt in the project directory

1. Verify that you're in an empty directory with `ls -al`
2. Create a virtual environment with these commands

```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -i https://pypi.dev.sodadata.io "soda-postgres>=4.0.0.dev32"
```

1. Test the CLI with command `soda` You should see sub command help.

This installed a virtual environment inside the hidden `.venv` subfolder folder inside the project directory

After installation, the virtual environment can be activated with `source .venv/bin/activate` 
and deactivated with `deactivate`

To test if the installation was successful, run `soda` and you should see some help for the [CLI](./cli.md)
```
> soda
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
...
```
