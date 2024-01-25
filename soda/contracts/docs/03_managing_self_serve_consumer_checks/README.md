# Self-serve consumer checks

> Content summary: This section should answer questions like:
> * What are self-serve consumer checks?
> * Why self-serve consumer checks and for whom?
> * When to use self-serve consumer checks and when not?
> * How does it relate to observability and contracts?

Self-serve consumer checks enable anyone in the company to prevent data issue by contributing 
data quality checks. There is an easy-to use user interface in Soda Cloud to compose checks.  
It's used by consumers or less technical users to protect their data products or other usage 
of the analytical data.

Self-serve consumer checks can be activated by executing them on a schedule.  The consumer / author 
can be notified of any check failures.

Self-serve consumer checks are an alternative for data contracts which is easier to use, more 
guidance, but a little less flexible. If there is a contract on the dataset of a self-serve 
consumer check, the consumer can propose to shift the check left.  That means asking the producer 
to migrate the check into the contract.  This way the producer takes on the ownership of the check 
and guarantees that the data will be checked for this property going forward.

Similar as alert notifications for contracts, also the notifications coming from self-serve will 
contain a link to diagnose the check failure and find the root cause.
