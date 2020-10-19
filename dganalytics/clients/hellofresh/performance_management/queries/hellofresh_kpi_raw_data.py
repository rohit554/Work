from dganalytics.utils.utils import exec_mongo_pipeline
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

pipeline = [
    {
        "$project": {
            "_id": 0,
            "user_id": 1.0,
            "user_name": 1.0,
            "report_date": 1.0,
            "key_word": 1.0,
            "not_responding_time": 1.0,
            "csat": 1.0,
            "org_id": 1.0
        }
    }
]

schema = StructType([StructField('csat', DoubleType, True),
                     StructField('key_word', DoubleType, True),
                     StructField('not_responding_time', DoubleType, True),
                     StructField('org_id', StringType, True),
                     StructField('report_date', TimestampType, True),
                     StructField('user_id', StringType, True),
                     StructField('user_name', StringType, True)])


def get_kpi_raw_data(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'All_Data',
                             schema, mongodb='hellofresh-prod')
    df.registerTempTable("all_data")
    df = spark.sql("""
                    select  user_id as userId,
                            report_date as reportDate,
                            user_name as userName,
                            csat as csat,
                            key_word as keyWord,
                            not_responding_time as notRespondingTime,
                            'hellofresh' orgId
                    from all_data
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.hellofresh_kpi_raw_data")
