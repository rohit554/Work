from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, DoubleType, FloatType, DateType
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from datetime import datetime, timedelta
from pyspark.sql.functions import lower, to_timestamp, col, from_unixtime
schema = StructType([
    StructField('coin_id', StringType(), True),
    StructField('coins_earned', DoubleType(), True),
    StructField('conversion_rate', DoubleType(), True),
    StructField('converted_points', DoubleType(), True),
    StructField('outcome_quantity', DoubleType(), True),
    StructField('processed_date', StringType(), True),
    StructField('orgId', StringType(), True),
    StructField('status', BooleanType(), True),
    StructField('userid', StringType(), True)
])
def get_userreward(spark):
    extract_start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        org_id = org_timezone['org_id']
        org_timezone = org_timezone['timezone']
        pipeline = [
            {
                "$match": {
                    'org_id': org_id,
                    '$expr': {
                        '$or': [
                            {
                                '$gte': [
                                    '$createdAt',
                                    {
                                        "$dateFromString": {
                                            "dateString": extract_start_time,
                                            "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                        }
                                    }
                                ]
                            }, {
                                '$gte': [
                                    '$updatedAt',
                                    {
                                        "$dateFromString": {
                                            "dateString": extract_start_time,
                                            "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }
            },
            {
                '$unwind': {
                    'path': '$coin_conversion',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$project': {
                    'coin_id': '$coin_conversion._id',
                    'coins_earned': '$coin_conversion.coins_earned',
                    'conversion_rate': '$coin_conversion.conversion_rate',
                    'converted_points': '$coin_conversion.converted_points',
                    'outcome_quantity': '$coin_conversion.outcome_quantity',
                    'processed_date': {
                        '$dateToString': {
                            'format': '%Y-%m-%dT%H:%M:%SZ',
                            'date': {'$toDate': '$coin_conversion._id'},
                            'timezone': org_timezone
                        }
                    },
                    'status': '$coin_conversion.status',
                    'orgId':'$org_id',
                    'userid': '$user_id'
                }
            }
        ]
        df = exec_mongo_pipeline(spark, pipeline, 'userRewards', schema)
        df = df.withColumn("orgId", lower(df["orgId"]))
        df.createOrReplaceTempView("userRewards")
        spark.sql("""
                    MERGE INTO dg_performance_management.userrewards a
                    USING userRewards b
                    ON a.coin_id = b.coin_id
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *        
                    """)
