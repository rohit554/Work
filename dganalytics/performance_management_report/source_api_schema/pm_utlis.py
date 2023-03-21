from typing import List
import requests
from pyspark.sql import SparkSession
import time
import datetime
import base64
import os
import shutil
import json
from pathlib import Path
from pyspark.sql.types import StructType
import argparse
import math
from dganalytics.connectors.gpc_v2.gpc_api_config import gpc_end_points
from dganalytics.utils.utils import get_path_vars, get_logger, delta_table_partition_ovrewrite, delta_table_ovrewrite
from pyspark.sql.functions import lit, monotonically_increasing_id, to_date, to_timestamp
import gzip
from dganalytics.utils.utils import get_secret
import tempfile

retry = 0

def gpc_utils_logger(tenant, app_name):
    global logger
    logger = get_logger(tenant, app_name)
    return logger


def get_dbname(tenant: str):
    db_name = "gpc_{}".format(tenant)
    return db_name


def get_schema(api_name: str):
    logger.info(f"read spark schema for {api_name}")
    schema_path = os.path.join(
        Path(__file__).parent, 'source_api_schemas', '{}.json'.format(api_name))
    with open(schema_path, 'r') as f:
        schema = f.read()
    schema = StructType.fromJson(json.loads(schema))
    return schema