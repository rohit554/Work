from pyspark.sql import SparkSession


def dim_surveys(spark: SparkSession, extract_date, extract_start_time, extract_end_time, tenant):
    surveys = spark.sql("""
                    insert overwrite dim_surveys
                        select distinct id as surveyId, name as surveyName,
                            description as surveyDescription from raw_surveys
                    """)
