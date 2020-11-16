from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os


def export_users_info(spark: SparkSession, tenant: str):

    df = spark.sql("""
            select department, division.id as divisionKey, division.id as divisionId, division.name as divisionName, 
email, id as userKey, id as userId, manager.id as managerKey, manager.id as managerId,
name, state, title, userName, version from gpc_hellofresh.raw_users 

    """)

    return df
