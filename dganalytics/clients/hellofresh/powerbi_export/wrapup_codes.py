from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os


def export_wrapup_codes(spark: SparkSession, tenant: str, region: str):

    df = spark.sql("""
            select wrapupId as wapupCodeKey, wrapupCode as wrapupDescription from gpc_hellofresh.dim_wrapup_codes
    """)
    tenant_path, db_path, log_path = get_path_vars('hellofresh')
    realtime_config = spark.sql("""
            select distinct wrapupId as wrapup_code_id, wrapupCode as wrapup_description from gpc_hellofresh.dim_wrapup_codes
    """).toPandas()
    realtime_config.to_csv(os.path.join(tenant_path, 'data', 'config', 'realtime_wrapupup_codes.csv'),
                           header=True, index=False)
    return df
