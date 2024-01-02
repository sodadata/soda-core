# YAML string to YAML string conversion

We translate Soda data contract YAML format to SodaCL YAML string first and then feed the SodaCL YAML
string into a Soda scan.  This way we can quickly build a relative complete coverage of checks 
in a contract with a tested implementation.

Pros:
* Easier & faster to build.
* More coverage and less chance of bugs
* Users can review the intermediate SodaCL and debug that based on the SodaCL docs.

Cons:
* No native error messages on the contract YAML lines.
* Extra 'compilation' step

Later we may consider to build native implementations for contracts to enable further improvements.
