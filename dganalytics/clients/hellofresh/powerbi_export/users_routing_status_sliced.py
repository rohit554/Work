from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd

def export_users_routing_status_sliced(spark: SparkSession, tenant: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.registerTempTable("user_timezone")

    df = spark.sql("""
            select frs.userId userKey,
from_utc_timestamp(frs.startTime, trim(ut.timeZone)) startTime,
from_utc_timestamp(frs.endTime, trim(ut.timeZone)) endTime,
routingStatus routingStatus,
from_utc_timestamp(frs.startTime, trim(ut.timeZone)) timeSlot,
1800 timeDiff,
'users_routing_status' pTableFlag
from gpc_hellofresh.fact_routing_status frs, user_timezone ut
    where frs.userId = ut.userId
    """)

    return df
