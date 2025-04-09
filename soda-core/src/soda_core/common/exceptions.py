class SodaCloudAuthenticationFailedException(Exception):
    """Indicates the authentication to Soda Cloud failed."""


class InvalidSodaCloudConfigurationException(Exception):
    """Indicates missing required keys in the Soda Cloud configuration file."""
