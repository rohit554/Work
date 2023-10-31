from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite,get_active_organization_timezones
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

schema = StructType([StructField('campaignId', StringType(), True),
                    StructField('campaignName', StringType(), True),
                    StructField('activityName', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('isChallengeActivty', BooleanType(), True),
                    StructField('orgId', StringType(), True)])

def get_activity_mapping(spark):
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
      org_id = org_timezone['org_id']
      org_timezone = org_timezone['timezone']
      pipeline = [
        {
          '$match': {
            "org_id" :  org_id,
            '$expr': {
                '$or': [
                    {
                        '$gte': [
                            '$creation_date', { '$date': extract_start_time }
                        ]
                    }, {
                        '$gte': [
                            '$modified_date', { '$date': extract_start_time }
                        ]
                    }
                ]
            }
          }
        },
        {
            '$lookup': {
                'from': 'Campaign', 
                'localField': 'campaign_id', 
                'foreignField': '_id', 
                'as': 'Campaign'
            }
        }, {
            '$unwind': {
                'path': '$Campaign', 
                'preserveNullAndEmptyArrays': False
            }
        }, {
            '$project': {
                "campaignId": '$campaign_id', 
                'campaignName': '$Campaign.name', 
                'activityName': '$name', 
                'activityId': '$_id', 
                'isChallengeActivty': '$challenge_flag', 
                'orgId': '$org_id'
            }
        }
      ]

      df = exec_mongo_pipeline(spark, pipeline, 'Outcomes', schema)
      df = df.withColumn("orgId", lower(df["orgId"]))
      df.createOrReplaceTempView("activity_mapping")
      
      spark.sql("""
            MERGE INTO dg_performance_management.activity_mapping AS target
            USING activity_mapping AS source
            ON target.orgId = source.orgId
            AND target.campaignId = source.campaignId
            AND target.activityId = source.activityId
            WHEN NOT MATCHED THEN
             INSERT *        
          """)

      