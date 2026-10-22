import logging

soda_logger: logging.Logger = logging.getLogger("soda")
logger: logging.Logger = soda_logger


class Emoticons:
    CROSS_MARK: str = "\u274c"
    WHITE_CHECK_MARK: str = "\u2705"
    WARNING: str = "\u26a0"
    CLOUD: str = "\u2601"
    OK_HAND: str = "\U0001f44c"
    SCROLL: str = "\U0001f4dc"
    FINGERS_CROSSED: str = "\U0001f91e"
    EXPLODING_HEAD: str = "\U0001f92f"
    POLICE_CAR_LIGHT: str = "\U0001f6a8"
    SEE_NO_EVIL: str = "\U0001f648"
    PINCHED_FINGERS: str = "\U0001f90c"
    QUESTION_MARK: str = "\u2753"


class ExtraKeys:
    LOCATION: str = "location"
    DOC: str = "doc"
    EXCEPTION: str = "exception"
    # Truthy on a record that carries group-by group keys or values and must be redacted;
    # the log site sets it from its run's setting. See ``logging_configuration.group_values_scope``.
    GROUP_VALUES: str = "group_values"
