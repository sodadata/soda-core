# Overview

Soda provides a complete and guided approach to data quality. A complete 
data quality approach is a combination of observability, contracts and 
self-serve consumer checks. Each of those capabilities can be adopted independently or 
in combination.  We recommend that you implement all these capabilities in order as they are 
presented here.  

(1) [Configuring an observability platform](01_configuring_observability/README.md) is 
a great starting point for a data quality strategy. That's because diagnosing issues will 
always be needed, especially when no data monitoring has been set up like contracts or 
self-serve consumer checks. We refer to observability as reactive and to data monitoring 
as preventative.

(2) [Implementing data contracts](02_implementing_data_contracts/README.md) helps data 
producers to build reliable pipelines that proactively alert whenever something is off 
with the data and even can stop the pipeline.

(3) [Managing self-serve consumer checks](03_managing_self_serve_consumer_checks/README.md) enable anyone in the 
organization to contribute their data domain knowledge. Data quality checks can be created 
and operated through an intuitive user interface targeted for consumers.  They can  
optionally request data producers to take over their checks into the contracts, which is 
a concrete path to shift-left. 
