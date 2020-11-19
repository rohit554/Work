from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd

def export_wfm_actuals(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.registerTempTable("user_timezone")

    df = spark.sql("""
           select
                    fw.userId userKey,
                    from_utc_timestamp(fw.startDate, trim(ut.timeZone)) startDate,
                    from_utc_timestamp(fw.endDate, trim(ut.timeZone)) endDate,
                    from_utc_timestamp(fw.endDate, trim(ut.timeZone)) actualsEndDate,
                    fw.actualActivityCategory actualActivityCategory,
                    0 endOffsetSeconds,
                    0 startOffsetSeconds
            from gpc_hellofresh.fact_wfm_actuals fw, user_timezone ut
            where fw.userId = ut.userId
            limit 100000
    """)
    return df
