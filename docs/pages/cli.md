# The Soda Core Contract CLI

### Step 1: Install Soda Core in a virtual environment

See [Install Soda Core in a Python virtual environment](./install.md)

### Step 2: Start a postgres database

If you already have a PostgreSQL database available, you can use that one.
If not, these instruction may help you to get one running on your machine:

Copy this docker compose file locally: https://github.com/sodadata/soda-core/blob/v4/soda-postgres/docker-compose.yml
Or copy the content here:

```yaml
version: "3.7"
services:
  soda-sql-postgres:
    image: postgres:9.6.17-alpine
    ports:
      - "5432:5432"
    volumes:
      - ./.postgres/:/var/lib/postgresql/data
    environment:
      - POSTGRES_USER=***
      - POSTGRES_DB=***
      - POSTGRES_HOST_AUTH_METHOD=trust
```

In this file, replace the credentials `***` with values of your choice.  You'll need to configure those
values in the data source configuration later, preferably via environment variables.

To start the postgres database with the above docker file, use command
```
docker-compose -f soda-postgres/docker-compose.yml up --remove-orphans
```

### Step 3: Create a table in the postgres db

```yaml
CREATE SCHEMA IF NOT EXISTS clitest AUTHORIZATION CURRENT_USER;
CREATE TABLE "soda_test"."clitest"."dim_employees" (
  id VARCHAR(255),
  age INT
);
INSERT INTO "soda_test"."clitest"."dim_employees" VALUES
  ('1',1),
  (NULL,-1),
  ('3',NULL),
  ('X',2);
```

### Step 4: Create a data source configuration YAML file

Use the Soda CLI to create a skeleton data source configuration file

`soda create-data-source -f ds.yml`

Expected command output

```
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
Creating postgres data source YAML file 'ds.yml'
✅ Created data source file 'ds.yml'
```

Update the information in the generated file

### Step 5: Test your data source configuration file

`soda test-data-source -ds ds.yml`

Expected command output

```
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
Testing data source configuration file ds.yml
✅ Success! Connection in 'ds.yml' tested ok.
```

### Step 6: Create the Soda Cloud configuration YAML file

`soda create-soda-cloud -f sc.yml`

Expected command output

```
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
Creating Soda Cloud YAML file 'sc.yml'
✅ Created Soda Cloud configuration file 'sc.yml'
```

Update the information in that file with a key id and secret that you obtain as follows:

https://dev.sodadata.io/ -> SSO -> organization: soda-sso-okta

To get credentials:

```
1) https://dev.sodadata.io/ -> avatar in top right corner -> Profile -> API keys

2) Test if you have permissions. By default it should.  If not, ask around.
```

For more, check out [the Soda Cloud YAML configuration file page](soda_cloud.md) 

### Step 7: Test your Soda Cloud configuration YAML file

`soda test-soda-cloud -sc sc.yml`

Expected command output:

```
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
Testing soda cloud file sc.yml
✅ Success! Tested Soda Cloud credentials in 'sc.yml'
```

### Step 8: Create a contract file

Create contract file `c.yml` with the contents below

The parser still has to be updated to the new language as we agreed. But for now, it's this syntax.

```yaml
datasource: postgres_ds
dataset: dim_employees
dataset_prefix: [soda_test, clitest]
columns:
  - name: id
    valid_values: ['1', '2', '3']
    checks:
      - invalid:
  - name: age
checks:
  - schema:
```

For more contract YAML examples, see

* https://github.com/sodadata/soda-core/blob/v4/docs/README.md
* And the test suite https://github.com/sodadata/soda-core/tree/v4/soda-core/tests/soda\_core/tests/features

### Step 9: Test the contract YAML syntax

`soda test-contract --data-source ds.yml --contract c.yml`

`soda test-contract -ds ds.yml -c c.yml`

Expected command output:
```text
  __|  _ \|  \   \
\__ \ (   |   | _ \
____/\___/___/_/  _\ CLI 4.0.0.dev??
Testing contract 'c.yml' YAML syntax
✅ All provided contracts are valid
```

### Step 10: Verify the contract locally

**a) Verifying locally**

`soda verify --data-source ds.yml --contract c.yml`

`soda verify -ds ds.yml -c c.yml`

This command will...

* Use ds.yml as the data source YAML configuration file
* Use c.yml as the contract YAML file
* Execute the queries & evaluate the results
* Print the check results on the console
* Not send anything to Soda Cloud

**b) Verifying locally and send the results to Soda Cloud**

Basically append `-sc sc.yml` to the previous command to send contract verification results to Soda Cloud. If you provide a Soda Cloud configuration file, by default the contract will be published and the contract verification results will be sent.

`soda verify --data-source ds.yml --soda-cloud sc.yml --contract c.yml`

`soda verify -ds ds.yml -sc sc.yml -c c.yml`

This command will

* Use ds.yml as the data source YAML configuration file
* Use sc.yml as the soda cloud YAML configuration file
* Use c.yml as the contract YAML file
* Execute the queries & evaluate the results
* Print the check results on the console
* Send the results to Soda Cloud. Eg https://dev.sodadata.io/
* Publish the contract on Soda Cloud

> Later might add command options to skip contract publication or sending the results to Soda Cloud when a Soda Cloud configuration is provided.

### Step 11: Verify the contract on Soda Agent

Prerequisites:

* A Soda Cloud account and credentials in the form of a Soda API key & secret
  * The user associated with the API key must have permissions.
    * if the contract is updated, user needs "Publish Contracts" and "Execute Contracts"
    * if the contract is not updated, "Execute Contracts"
* A Soda Cloud data source with the same name as referred to in the contract

`soda verify -a -sc sc.yml -c c.yml`

`soda verify --use-agent --soda-cloud sc.yml --contract c.yml`

This command will

* Use sc.yml as the soda cloud YAML configuration file
* Use c.yml as the contract YAML file
* Wait until the contract verification is completed
  * Publish the contract file on Soda Cloud
    * Ensure the datasource & dataset exist on Soda Cloud. Create if necessary.
    * Ensure the contract file is the latest version of the contract associated with the dataset
  * Soda Cloud will delegate execution of contract verification to the Soda Agent
  * Soda Agent will verify the contract and send the results to Soda Cloud.
  * The CLI will poll Soda Cloud until the contract verification is finished.
  * The CLI will poll fetch the logs from Soda Cloud
* CLI print the logs and check results on the console

### Specifying variables in the CLI

`soda contract verify --set key=value ...`
