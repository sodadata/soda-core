# Soda data contracts

Data Contracts are the best assurance to ensure your data pipelines produce reliable data and 
prevent data issues from occurring. 

Data contracts starts with a description of the data in a YAML file. It includes information like 
the schema (columns & data type) and checks for aspects like freshness, missing values, validity and so on.
These checks form a description of how the data should look like.  Each time new data is produced, the 
new data can be verified against the contracts. If one of the checks fails, the data producer can 
investigate. This prevents that unexpected changes to the data lead to issues with often a big impact.

A data contracts also implies there is at all times a trustworthy description of the data in the form 
of a YAML file.  It can be used to push information to a catalog.

Data contracts enable encapsulation in the software engineering sense. Consider data pipelines as a set of
components. For example, a dbt transformation is a component that takes datasets as input and produces
a set of output datasets.  The people or team responsible for the dbt transformation are also responsible
for the contract of the data produced by the dbt transformation.  Contracts are a strategy to divide and
conquer the big monolithic data pipelines into manageable pipeline components.

Apart from verifying the new data in the production pipeline, contracts can also be verified in CI/CD to 
ensure that new code changes don't bring unexpected changes to the data. This fits in a prevention strategy 
to find and resolve data issues as soon as possible.

Get started: 

* [Contract basics](01_contract_basics.md)
  * [Simple contract file explained](01_contract_basics.md#simple-contract-file-explained)
  * [Soda contract file naming convension](01_contract_basics.md#soda-contract-file-naming-convension)
  * [Setting up code completion in PyCharm](01_contract_basics.md#setting-up-code-completion-in-pycharm)
  * [Setting up code completion in VSCode](01_contract_basics.md#setting-up-code-completion-in-vscode)
* [Writing contract checks](02_writing_contract_checks.md)
* [Verifying a contract](03_verifying_a_contract.md)
* [Setting up notifications](04_setting_up_notifications.md)
