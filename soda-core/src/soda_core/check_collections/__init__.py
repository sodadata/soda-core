"""Check collections — generalized verification engine.

A check collection is one verifiable YAML file: a contract, a data standard, or
any future subtype. ``CheckCollectionImpl`` is the engine; subclasses declare
four plain class attributes (``wire_source``, ``display_name``, ``yaml_class``,
``result_class``) and inherit ``__init__`` / ``verify`` / etc.
"""
