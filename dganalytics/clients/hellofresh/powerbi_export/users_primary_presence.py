from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd


def export_users_primary_presence(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(
        tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.registerTempTable("user_timezone")

    df = spark.sql("""
            select 
                fp.userId as UserKey, fp.userId, 
                    from_utc_timestamp(fp.startTime, trim(ut.timeZone)) startTime,
                    from_utc_timestamp(fp.endTime, trim(ut.timeZone)) endTime,
                    fp.systemPresence,
                    'users_primary_presence' pTableFlag
                from fact_primary_presence fp, user_timezone ut
                where fp.userId = ut.userId
    """)

    return df
