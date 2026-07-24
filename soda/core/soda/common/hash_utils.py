import hashlib


def fips_safe_hasher(digest_size: int):
    """
    Returns a hash object that mimics hashlib.blake2b.
    Falls back to hashlib.sha256 with truncation if blake2b is unavailable (e.g., in FIPS mode).
    """
    try:
        return hashlib.blake2b(digest_size=digest_size)
    except (TypeError, ValueError, AttributeError):

        class TruncatedSHA256:
            def __init__(self):
                self._hasher = hashlib.sha256()
                self.digest_size = digest_size

            def update(self, data):
                self._hasher.update(data)

            def digest(self):
                return self._hasher.digest()[: self.digest_size]

            def hexdigest(self):
                return self._hasher.hexdigest()[: self.digest_size * 2]

        return TruncatedSHA256()
