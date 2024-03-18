# New YAML framework

See `../soda/contracts/impl/yaml.py`

The new YAML abstract allows for:
* Capturing all errors into a central logs object instead of raising an exception on the first problem
* Convenience read_* methods on the YamlObject for writing parsing code
* A more convenient way to access the line and column information (location)

It's intended for reading, not writing. Should we add ability to write on this same framework?
For now we write using plain dicts/lists.  There is also the unpack() method.
But full mutable data structures would require overloading the muting operators like eg __setitem__ etc
