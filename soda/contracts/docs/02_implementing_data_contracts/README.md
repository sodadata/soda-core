# Implementing data contracts

> Content summary: This section should answer questions like:
> * What are contracts?
> * Why contracts and for whom?
> * When to use contracts and when not?
> * How does it relate to observability and self-serve?

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

## 1) Verifying a contract using the library

## 2) Verifying contracts using Soda Agent

Verifying 

## 3) Writing contracts

## 4) Enabling contract requests in Soda Cloud UI
