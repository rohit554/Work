from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os

def export_users_routing_status(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = spark.read.option("header", "true").csv(
        os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'))
    user_timezone.createOrReplaceTempView("user_timezone")

    df = spark.sql("""
            SELECT 
                fp.userId as UserKey, fp.userId, 
                from_utc_timestamp(fp.startTime, trim(ut.timeZone)) startTime,
                from_utc_timestamp(fp.endTime, trim(ut.timeZone)) endTime,
                fp.routingStatus 
            FROM 
                fact_routing_status fp, 
                user_timezone ut
            WHERE 
                fp.userId = ut.userId
    """)

    return df