from abc import ABC, abstractmethod
from pathlib import Path


class BaseReader(ABC):
    BASE_CONFIGURATION_FOLDER = f'{Path.home()}/.soda'
