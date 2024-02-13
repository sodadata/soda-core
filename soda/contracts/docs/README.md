# Implementing data contracts

Data Contracts are the best tool to prevent data issues from occurring. Data contracts
is a way for data producers to take ownership of the contract and communicate their commitments. 
A contract can contains information like the schema, owner, arrival times, data quality checks and 
is used for the following use cases:

1) A reliable source of rich metadata for data discovery tools
2) Implement alerting or even circuit breaking if new data is not compliant
3) Verify the contract on pipeline code changes and alert the producer if not compliant

### Learn about contract verification

* [Contract basics](01_contract_basics.md)
  * [Simple contract file explained](01_contract_basics.md#simple-contract-file-explained)
  * [Soda contract file naming convension](01_contract_basics.md#soda-contract-file-naming-convension)
  * [Setting up code completion in PyCharm](01_contract_basics.md#setting-up-code-completion-in-pycharm)
  * [Setting up code completion in VSCode](01_contract_basics.md#setting-up-code-completion-in-vscode)
* [Writing contract checks](02_writing_contract_checks.md)
* [Verifying a contract](03_verifying_a_contract.md)
* [Setting up notifications](04_setting_up_notifications.md)

### How to start rolling out data contracts

The contract defines an interface that enables consumers to query a dataset with SQL. While most data engineering 
teams limit to reactive approaches to data quality, contracts are the best way to prevent data issues.  It's a 
technique to build reliable data infrastructure that scales from small to large data engineering teams. 

Data contracts enable encapsulation in the software engineering sense,  Consider data pipelines as a set of 
components. For example, a dbt transformation is a component that takes datasets as input and produces 
a set of output datasets.  The people or team responsible for the dbt transformation are also responsible 
for the contract of the data produced by the dbt transformation.  Contracts are a strategy to divide and 
conquer the big monolithic data pipelines into manageable pipeline components.

If you don't know where to start, start right.  Find your most relevant data product that is using analytical 
data for a business goal. Like for instance a BI report or a machine learning model.  Identify the datasets 
it directly consumes.  Find the team that's responsible for producing those datasets.  Whom would you talk 
to if you suspect something is wrong with that data?  Ask that person or team to start building a contract 
for that dataset.  

Those people or teams might get uneasy to get all the responsibility while they can't be sure about their 
input datasets.  That's the next step to roll out contracts. Leave it to the producers to push for contracts 
on their inputs.  They first ask the teams responsible for their input data to supply contracts for those 
datasets. Lacking those people they can protect themselves and build those contracts themselves.  
