from dganalytics.utils.utils import exec_mongo_pipeline
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

pipeline = [
    {
        "$project": {
            "_id": 0.0,
            "user_id": 1.0,
            "login_attempt": 1.0,
            "date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$timestamp"
                }
            }
        }
    }
]

schema = StructType([StructField('date', StringType(), True),
                     StructField('login_attempt', IntegerType(), True),
                     StructField('user_id', StringType(), True)])


def get_logins(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Audit_Log', schema)
    df.registerTempTable("logins")
    df = spark.sql("""
                    select  cast(date as date) date,
                            login_attempt loginAttempt,
                            user_id userId,
                            'salmatcolesonline' orgId
                    from logins
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.logins")
