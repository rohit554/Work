from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, TimestampType, DoubleType, FloatType, DateType
from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from datetime import datetime, timedelta
from pyspark.sql.functions import lower, to_timestamp, col, from_unixtime

schema = StructType([
    StructField('reedemId', StringType(), True),
    StructField('userMongoId', StringType(), True),
    StructField('coinsUsed', DoubleType(), True),
    StructField('campaignId', StringType(), True),
    StructField('teamId', StringType(), True),
    StructField('amountCharged', DoubleType(), True),
    StructField('creation_date', StringType(), True),
    StructField('orgId', StringType(), True)
])

def get_redeemedvouchers(spark):
    extract_start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'

    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
        org_id = org_timezone['org_id']
        org_timezone = org_timezone['timezone']
        pipeline = [
                    {
                        '$match': {
                            'org_id': org_id
                        }
                    }, {
                        '$project': {
                            'reedemId': '$_id', 
                            'userMongoId': 1, 
                            'coinsUsed': 1, 
                            'campaignId': 1, 
                            'teamId': 1, 
                            'amountCharged': 1, 
                            'creation_date':{
                                          '$dateToString': {
                                          'format': '%Y-%m-%dT%H:%M:%SZ',
                                          'date': '$creation_date',
                                          'timezone': org_timezone
                                            }
                                        },
                            'orgId':'$org_id', 
                            '_id': 0
                        }
                    }
                ]
        
        df = exec_mongo_pipeline(spark, pipeline, 'xoxodayRedeemedVouchers', schema)
        df = df.withColumn("orgId", lower(df["orgId"]))
        df.createOrReplaceTempView("RedeemedVouchers")
        spark.sql("""
                    MERGE INTO dg_performance_management.xoxodayredeemedvouchers a
                    USING RedeemedVouchers b
                    ON a.reedemId = b.reedemId
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *        
                    """)