from __future__ import annotations


class SodaException(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaConnectionException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class SodaCloudException(SodaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
