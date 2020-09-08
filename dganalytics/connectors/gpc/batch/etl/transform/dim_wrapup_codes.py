from pyspark.sql import SparkSession


def dim_wrapup_codes(spark: SparkSession, extract_date: str):
    users = spark.sql("""
                    insert overwrite dim_wrapup_codes 
                        select  
                            distinct id as wrapupId, 
                            name as wrapupCode  
                                from raw_wrapup_codes
                    """)
