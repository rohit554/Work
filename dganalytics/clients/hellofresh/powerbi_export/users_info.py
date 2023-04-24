from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os


def export_users_info(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            SELECT 
                  a.department, 
                  a.division.id AS divisionKey,
                  COALESCE(a.employerInfo.dateHire, (SELECT MIN(extractDate) FROM gpc_hellofresh.raw_users_details b WHERE a.id = b.userId)) AS hireDate, 
                  a.division.id AS divisionId, 
                  a.division.name AS divisionName, 
                  a.email, id AS userKey, 
                  a.id AS userId, 
                  a.manager.id AS managerKey, 
                  a.manager.id AS managerId,  
                  a.name, 
                  a.state, 
                  a.title, 
                  a.userName, 
                  a.version 
            FROM gpc_hellofresh.raw_users a 

    """)

    return df
