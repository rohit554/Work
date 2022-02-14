from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd

def export_wfm_day_metrics(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    df = spark.sql(f"""
            select
                from_utc_timestamp(fw.endDate, trim(ut.timeZone)) actualsEndDate,
                from_utc_timestamp(fw.endDate, trim(ut.timeZone)) endDate,
                fw.impact impact,
                from_utc_timestamp(fw.startDate, trim(ut.timeZone)) startDate,
                fw.userId userKey,
                fw.actualLengthSecs,
                0 adherencePercentage,
                fw.adherenceScheduleSecs,
                fw.conformanceActualSecs,
                0 conformancePercentage,
                fw.dayStartOffsetSecs,
                fw.exceptionCount,
                fw.exceptionDurationSecs,
                fw.impactSeconds,
                fw.scheduleLengthSecs
            from gpc_hellofresh.fact_wfm_day_metrics fw, user_timezone ut
            where fw.userId = ut.userId
            and ut.region {" = 'US'" if region == 'US' else " <> 'US'"}
    """)

    return df
