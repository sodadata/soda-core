import re
from datetime import datetime, timezone
from typing import Optional


def convert_datetime_to_str(dt: datetime) -> Optional[str]:
    try:
        # First, we check if the datetime has timezone info
        if dt.tzinfo is None:
            # If dt does not have timezone info, Python will assume dt is in the local system timezone (e.g. UTC+1)
            # Then we need to REPLACE (not astimezone) the timezone to UTC
            new_dt = dt.replace(tzinfo=timezone.utc)
        else:
            # The datetime has timezone info, we need to **convert** it to UTC (to be consistent with the rest of the code)
            new_dt = dt.astimezone(timezone.utc)
        return new_dt.isoformat(timespec="seconds")

        # OLD CODE: to remove
        # Attention, if dt does not have timezone info, Python will assume dt is in the local system timezone (e.g. UTC+1)
        # return dt.astimezone(timezone.utc).isoformat(timespec="seconds")
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
