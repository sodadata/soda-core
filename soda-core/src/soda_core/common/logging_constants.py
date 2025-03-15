import logging

soda_logger: logging.Logger = logging.getLogger("soda")
logger: logging.Logger = soda_logger


class Emoticons:
    CROSS_MARK: str = "\u274C"
    WHITE_CHECK_MARK: str = "\u2705"
    CLOUD: str = "\u2601"
    OK_HAND: str = "\U0001F44C"
    SCROLL: str = "\U0001F4DC"
    FINGERS_CROSSED: str = "\U0001F91E"
    EXPLODING_HEAD: str = "\U0001F92F"
    POLICE_CAR_LIGHT: str = "\U0001F6A8"
    SEE_NO_EVIL: str = "\U0001F648"
    PINCHED_FINGERS: str = "\U0001F90C"


class ExtraKeys:
    LOCATION: str = "location"
    DOC: str = "doc"
    EXCEPTION: str = "exception"
