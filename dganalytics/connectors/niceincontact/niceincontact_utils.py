

import os
from os.path import expanduser
from dganalytics.utils.utils import get_logger
from pyspark.sql.types import StructType
import json
from pathlib import Path

def niceincontact_utils_logger(tenant, app_name):
    """
    Initialize and return a logger for the given tenant and application name.

    This function sets a global `logger` variable using the `get_logger` utility,
    allowing other functions in the module to reuse it.

    Args:
        tenant (str): The tenant identifier.
        app_name (str): The name of the application.

    Returns:
        Logger: A configured logger instance for the given tenant and application.
    """
    global logger
    logger = get_logger(tenant, app_name)
    return logger

def get_dbname(tenant: str, app_name: str = "niceincontact") -> str:
    """
    Generate a standardized database name using the application and tenant names.

    Args:
        tenant (str): The tenant identifier.
        app_name (str, optional): The application name prefix. Defaults to "niceincontact".

    Returns:
        str: A string representing the formatted database name.
    """
    db_name = f"{app_name}_{tenant}"
    return db_name

def get_schema(api_name: str) -> StructType:
    """
    Load the Spark schema for a given API from a corresponding JSON file.

    Args:
        api_name (str): The name of the API whose schema needs to be loaded.

    Returns:
        StructType: A PySpark StructType object representing the schema.

    Raises:
        FileNotFoundError: If the schema JSON file does not exist.
        ValueError: If the JSON content cannot be parsed into a StructType.
    """
    schema_path = os.path.join(
        Path(__file__).parent, 'source_api_schemas', f'{api_name}.json'
    )
    
    try:
        with open(schema_path, 'r') as f:
            schema_json = f.read()
        schema = StructType.fromJson(json.loads(schema_json))
        return schema
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    except Exception as e:
        raise ValueError(f"Failed to parse schema for API '{api_name}': {e}")