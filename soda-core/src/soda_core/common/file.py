import logging
import os

from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


def save_to_file(content: str, file_path: str) -> None:
    directory = os.path.dirname(file_path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(file_path, "w") as file:
        file.write(content)

    logger.info(f"Saved file '{file_path}'")
