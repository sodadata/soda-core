import logging
from typing import List, Optional
from soda.common.exceptions import DataSourceConnectionError

from soda.execution.data_source import DataSource

import itertools

import pyodbc
from collections import namedtuple
from enum import Enum
from pyhive import hive
from pyhive.exc import Error
from soda.execution.query import Query
from thrift.transport.TTransport import TTransportException
import logging
from typing import Any, Dict, List, Optional
from soda.common.logs import Logs
from soda.__version__ import SODA_CORE_VERSION
from soda.execution.data_type import DataType


class DataSourceImpl(DataSource):
    TYPE = "spark-df"
