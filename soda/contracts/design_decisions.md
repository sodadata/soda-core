# Soda data contract YAML format

### No data source & connection details

We leave datasource, connection and schema out of the contract files
  * Pro
    * The same contract file can be used to run in CI/CD
    * Leveraging the existing Soda configuration files
  * Con:
    * more CLI args
    * less repeatable
    * less sharing of the sharable connection details & profiles

### Column names as keys

* Nicer YAML. A list of columns with a `name` key is less readable.
* Still JSON Schema enforcable YAML

### Keys without spaces nor variables

No spaces in keys.  No parsing of keys.  No variable parts in keys except for the column names.

* Pro:
  * More JSON compliant
  * More validation from JSON schema
  * More expected and in line with people's expectations
* Con:
  * Not similar to SodaCL

# Implementation

### YAML string to YAML string conversion

We translate Soda data contract YAML format to SodaCL YAML string first and then feed the SodaCL YAML
string into a Soda scan.

Pros:
* Composable.
* Easier to build.
* Users can review the intermediate SodaCL and debug that based on the SodaCL docs.

Cons:
* No native error messages on the contract YAML lines.
* Extra 'compilation' step

### New YAML framework

See `soda/contracts/soda/contracts/yaml.py`
Is that a better YAML abstraction?
It's intended for reading, not writing. Should we add ability to write on this same framework?
For now we write using plain dicts/lists.  There is also the unpack() method.
But full mutable data structures would require overloading the muting operators like eg __setitem__ etc
