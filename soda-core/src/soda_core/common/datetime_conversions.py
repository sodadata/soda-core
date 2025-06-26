import re
from datetime import datetime, timezone
from typing import Optional


def convert_datetime_to_str(dt: datetime) -> Optional[str]:
    try:
        return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
    except Exception as e:
        return None


def convert_str_to_datetime(date_string: str) -> Optional[datetime]:
    """
    Returns the parsed datetime or None if there is a parsing error.
    No exceptions are raised
    """
    try:
        # fromisoformat raises ValueError if date_string ends with a Z:
        # Eg Invalid isoformat string: '2025-02-21T06:16:59Z'
        if date_string.endswith("Z"):
            # Z means Zulu time, which is UTC
            # Converting timezone to format that fromisoformat understands
            datetime_str_without_z: str = date_string[:-1]
            datetime_str_without_z_seconds = re.sub(r"\.(\d+)$", "", datetime_str_without_z)
            date_string = f"{datetime_str_without_z_seconds}+00:00"
        return datetime.fromisoformat(date_string)
    except (Exception, AttributeError) as e:
        return None
