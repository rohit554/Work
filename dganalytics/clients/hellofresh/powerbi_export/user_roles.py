from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os


def export_user_roles(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    df = spark.sql("""
            select  userKey, userId, roles.id roleKey, roles.id roleId, roles.name as roleName
                select id userKey, id userId, explode(authorization.roles) roles  from gpc_hellofresh.raw_users
    """)

    return df
