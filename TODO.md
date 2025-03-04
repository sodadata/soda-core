# TODOs

* [ ] Precommit
* [ ] License headers
* [ ] Consider how to split up logs by contract.  Which logs are shared (like data source and soda cloud).  And which are contract specific.  How should this reflect in sending to cloud?
* [ ] Define and test naming restrictions. What are the data source naming restrictions?  Same for the qualifiers.  All to ensure we can generate consistent check identities.
* [ ] Create a release procedure in [README.md](README.md#creating-a-new-release)
* [ ] Test & document doc links
* [ ] Consider excluding invalid values from reference check
* [ ] Consider adding index checking to the schema check
* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash
