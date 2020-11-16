from pyspark.sql import SparkSession

def export_wrapup_codes(spark: SparkSession, tenant: str):

    df = spark.sql("""
            select wrapupId as wapupCodeKey, wrapupCode as wrapupDescription from gpc_hellofresh.dim_wrapup_codes
    """)

    return df
