from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd

def export_wfm_actuals(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    user_timezone = pd.read_csv(os.path.join(tenant_path, 'data', 'config', 'User_Group_region_Sites.csv'), header=0)
    user_timezone = spark.createDataFrame(user_timezone)
    user_timezone.createOrReplaceTempView("user_timezone")

    df = spark.sql(f"""
           SELECT
                    fw.userId userKey,
                    from_utc_timestamp(fw.startDate, trim(ut.timeZone)) startDate,
                    from_utc_timestamp(fw.endDate, trim(ut.timeZone)) endDate,
                    from_utc_timestamp(fw.endDate, trim(ut.timeZone)) actualsEndDate,
                    fw.actualActivityCategory actualActivityCategory,
                    0 endOffsetSeconds,
                    0 startOffsetSeconds
            FROM gpc_hellofresh.fact_wfm_actuals fw, user_timezone ut
            WHERE fw.userId = ut.userId
            AND ut.region {" = 'US'" if region == 'US' else " <> 'US'"}
            AND CAST(from_utc_timestamp(fw.startDate, trim(ut.timeZone)) AS date) >= add_months(current_date(), -12)
    """)
    return df
