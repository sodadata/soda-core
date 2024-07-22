# Contract check identities

### From the user perspective

Check identity is used to correlate checks in files with Soda Cloud.

In contracts, we want to change the user interface regarding identities.

The contracts parser ensures that all checks in a contract must have a unique identity.
An error will be created if there are multiple checks with the same identity. An identity
will be automatically generated based on a few check properties including the name. If two
checks are not unique, users must use the name property to ensure uniqueness.

> IMPORTANT! All this means that users have to be aware of the Soda Cloud correlation impact when they
> change the name! Changing the name will also change the identity and hence will get a new check and
> check history on Soda Cloud.In the future we envision a mechanism for renaming a check without loosing
> the history by introducing a `name_was` property on a check. When users want to change the name, they
> will have to rename the existing `name` property to `name_was` and create a new `name` property with
> the new name.

Checks automatically generate a unique identity if you have max 1 check in each scope.
A scope is defined by
* data_source
* schema
* dataset
* column
* check type

So as long as you have only one check type in the same list of checks in the YAML, you're good.

In case of dataset checks like `metric_query` or `metric_expression`, it might be likely that
there are multiple checks with the same check type.  To keep those unique, a `name` is mandatory.

### Implementation docs

The contract check identity will be a consistent hash (soda/contracts/soda/contracts/impl/consistent_hash_builder.py) based on:

For schema checks:
* data_source
* schema
* dataset
* check type (=schema)

For all other checks:
* data_source
* schema
* dataset
* column
* check type
* check name

The check identity will be used as explicit `identity` in the generated SodaCL

Soda Core is updated so that it will pass the identity back as the property `source_identity` in the scan results.
The `source_identity` property in the scan results will also be used to correlate the Soda scan check results with
the contract checks for reporting and logging the results.
