import re

SODA_CORE_VERSION = "4.0.0.b1"


def clean_soda_core_version() -> str:
    return clean_semver(SODA_CORE_VERSION)


def clean_semver(version_str) -> str:
    match = re.match(r'^(\d+\.\d+\.\d+)', version_str)
    if match:
        return match.group(1)
    else:
        raise ValueError(f"Invalid version format: {version_str}")
