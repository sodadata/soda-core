from __future__ import annotations

from datetime import date, datetime
from hashlib import blake2b
from numbers import Number
from typing import Optional


class ConsistentHashBuilder:
    def __init__(self, hash_string_length: int = 8):
        if hash_string_length % 2 != 0:
            raise AssertionError(f"hash_string_length must be divisible by 2: {hash_string_length} is not")
        self.hash_string_length = hash_string_length
        self.blake2b = None

    def __get_blake2b(self) -> blake2b:
        # Lazy initialization of blake2b in order to return None in the self.get_hash(self) in case nothing was added
        if self.blake2b is None:
            self.blake2b = blake2b(digest_size=int(self.hash_string_length / 2))
        return self.blake2b

    def add(self, value: Optional[object]) -> ConsistentHashBuilder:
        if value is not None:
            if isinstance(value, str):
                self.__get_blake2b().update(value.encode("utf-8"))
            elif isinstance(value, dict):
                for key, value in value.items():
                    self.add_property(key, value)
            elif isinstance(value, list):
                for e in value:
                    self.add(e)
            elif isinstance(value, Number) or isinstance(value, bool):
                self.__get_blake2b().update(str(value).encode("utf-8"))
            elif isinstance(value, datetime):
                self.add(str(value))
            elif isinstance(value, date):
                self.add(str(value))
            else:
                raise AssertionError(f"Unsupported hash value type {value} ({type(value).__name__})")
        return self

    def add_property(self, key: str, value: Optional[object]) -> ConsistentHashBuilder:
        if value is not None:
            self.add(key)
            self.add(value)
        return self

    def get_hash(self) -> str:
        return self.blake2b.hexdigest() if self.blake2b else None
