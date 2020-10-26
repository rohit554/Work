from pyspark.sql import SparkSession


def dim_wrapup_codes(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    users = spark.sql("""
                    insert overwrite dim_wrapup_codes
                        select
                            distinct id as wrapupId,
                            name as wrapupCode
                                from raw_wrapup_codes
                    """)
