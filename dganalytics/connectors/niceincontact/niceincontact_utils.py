import os
from dganalytics.utils.utils import get_logger
from os.path import expanduser
from pyspark.sql.types import StructType
import json
from pathlib import Path



class NiceInContactClient:
    """
    Client for managing configuration and utilities related to NICE inContact data processing.

    This client sets environment-based paths, initializes logging, generates the database name,
    and provides a method to load Spark schemas from predefined JSON files.
    """
    def __init__(self, tenant: str, app_name: str, env: str = "prod"):
        """
        Initialize the NiceInContactClient.

        Args:
            tenant (str): The tenant identifier.
            app_name (str): Name of the application using the client.
            env (str): Deployment environment. Options: 'local', 'prod'. Defaults to 'prod'.
        """
        self.tenant = tenant
        self.app_name = app_name
        self.env = env.lower()
        self.prefix = "niceincontact"
        self.logger = get_logger(tenant, app_name)

        self.db_name = self._get_dbname()
        self.tenant_path = ""
        self.db_path = ""
        self.log_path = ""
        self._get_path_vars()

    def _get_path_vars(self):
        """
        Set environment-specific file paths for tenant data, database location, and logs.

        In 'local' mode, paths are relative to the user's home directory.
        In other environments, paths are set for DBFS (Databricks File System).
        """
        if self.env == "local":
            home = expanduser("~")
            self.tenant_path = os.path.join(home, "datagamz", "analytics", self.tenant)
            self.db_path = "file:///" + self.tenant_path.replace("\\", "/") + "/data/databases"
            self.log_path = os.path.join(self.tenant_path, 'logs')
        else:
            self.tenant_path = f"/dbfs/mnt/datagamz/{self.tenant}"
            self.db_path = f"dbfs:/mnt/datagamz/{self.tenant}/data/databases"
            self.log_path = f"/dbfs/mnt/datagamz/{self.tenant}/logs"

        self.logger.info(f"Path variables set for env={self.env}")
        self.logger.debug(f"tenant_path={self.tenant_path}, db_path={self.db_path}, log_path={self.log_path}")

    def _get_dbname(self):
        """
        Generate a standardized database name based on the tenant.

        Returns:
            str: The fully qualified database name.
        """
        return f"{self.prefix}_{self.tenant}"
    
    def get_schema(self, api_name: str) -> StructType:
        """
        Load the Spark schema for the given API from a JSON file.

        Args:
            api_name (str): Name of the API whose schema needs to be loaded.

        Returns:
            StructType: A PySpark StructType object representing the schema.

        Raises:
            FileNotFoundError: If the schema file is missing.
            Exception: For any other issues in reading/parsing the schema.
        """
        self.logger.info(f"Reading Spark schema for API: {api_name}")
        schema_path = os.path.join(
            Path(__file__).parent, 'source_api_schemas', f'{api_name}.json'
        )

        try:
            with open(schema_path, 'r') as f:
                schema_json = f.read()
            schema = StructType.fromJson(json.loads(schema_json))
            return schema
        except FileNotFoundError:
            self.logger.error(f"Schema file not found: {schema_path}")
            raise
        except Exception as e:
            self.logger.exception(f"Failed to load schema for {api_name}: {e}")
            raise
