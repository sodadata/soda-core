# Overview

Soda provides a complete and guided approach to data quality. A complete 
data quality approach is a combination of observability, contracts and 
self-serve consumer checks.

We recommend that you explore the capabilities in order.  If you don't 
have an [observability](#self-serve-consumer-checks) platform to diagnose 
data issues, that's a good place to start.

Once you have observability covered, next consider adopting [contracts](#enforce-contracts) 
as a data testing strategy for data producers.

And once that is on the rails, ensure that everyone in the business can 
contribute their domain knowledge, 
enable [self-serve consumer checks](#self-serve-consumer-checks).  


### Observability

> Content summary: This section should answer questions like:
> * What is observability?
> * Why start with observability?
> * When to use observability and when not?
> * How does it relate to contracts and self-serve?

If you are just getting started with data quality, chances are you 
are experiencing data quality issues and questions around data that 
need to be investigated by a data engineer.

Soda's observability platform is easy to set up and will help you 
diagnose any data issue by creating visibility into your data stack
automatically.

Using Soda's data observability, an engineer can start diagnosing any 
data issue.  A data issue can be an consumer that has noticed some weird data 
and asks to verify.  Another example is a alert notification of the data 
monitoring parts of Soda.  

Soda's observability platform enables engineers to start digging and perform 
the technical investigation to find root causes or answer any questions about 
the quality of data.

Of the three components needed for a data quality strategy, observability is the 
easiest to set up.  While observability is great to enable data issue debugging, 
it is not strategy to prevent data issues from occurring.   

See [Setting up observability](02_configuring_observability/setting_up_observability.md)

### Enforce contracts

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

See [Enforcing data contracts](./03_enforcing_data_contracts/00_enforcing_data_contracts.md)

### Self-serve consumer checks

> Content summary: This section should answer questions like:
> * What are self-serve consumer checks?
> * Why self-serve consumer checks and for whom?
> * When to use self-serve consumer checks and when not?
> * How does it relate to observability and contracts?

See [Self-serve consumer checks](04_managing_consumer_checks_in_soda_cloud/00_managing_consumer_checks_in_soda_cloud.md)
