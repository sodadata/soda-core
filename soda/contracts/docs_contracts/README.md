# Implementing data contracts

Data Contracts are the best tool to prevent data issues from occurring. Data contracts 
drives data producers to take ownership of the contract and the data.  A contract contains 
information like the owner, schema, arrival times, data quality checks and is used for the 
following use cases.

1) Source of information for data discovery tools 
2) Verify the contract on new data and alert the producer if not compliant
3) Verify the contract on pipeline code changes and alert the producer if not compliant

Use contracts if you want to prevent data issues. It's a tool used by the producer, which 
are the technical engineering team responsible for the pipeline component producing the data.  

It's a technical component used for encapsulation in the software engineering sense. A data 
contract specifies the interface of the software component producing the data.  The interface 
is the dataset that can be queried with SQL.  It can be used to divide and conquer 
the big monolithic data pipelines into manageable pipeline components.  

Contracts will significantly reduce the number of data issues that occur and it will 
help the diagnostic process.   

* First, choose one of the following ways to verify a contract
  * (1) [Verifying a contract using the library](01_verifying_a_contract_using_library/README.md)
  * (2) [Verifying contracts using Soda Agent](02_verifying_contracts_using_soda_cloud_agent/README.md)
  * (3) [Verifying contracts in an Airflow DAG](03_verifying_a_contract_in_an_airflow_dag/README.md)
  * (4) [Verifying a contract using docker](04_verifying_a_contract_using_docker/README.md)
  * (5) [Verifying a contract using GitHub action](05_verifying_a_contract_as_a_github_action/README.md)
* Next, learn how to write contract YAML files
  * (6) [Writing contracts](06_writing_contracts/README.md)
* And finally, enable contract check contributions from anyone in the organization
  * (7) [Enabling contract requests in Soda Cloud UI](07_enabling_contract_requests/README.md)
