from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_organization_timezones
from datetime import datetime, timedelta

schema = StructType([StructField('campaignId', StringType(), True),
                    StructField('campaignName', StringType(), True),
                    StructField('activityName', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('isChallengeActivty', BooleanType(), True),
                    StructField('orgId', StringType(), True)])

def get_activity_mapping(spark):
    Current_Date = datetime.now()
    extract_end_time = Current_Date.strftime('%Y-%m-%dT%H:%M:%S.%fZ') 
    extract_start_time = (Current_Date - timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

    for org_timezone in get_active_organization_timezones().rdd.collect():
      org_id = org_timezone['org_id']
      org_timezone = org_timezone['timezone']
      print(org_id," : ",org_timezone)
      pipeline = [
        {
          '$match': {
            "org_id" :  org_id,
            "$or":[
                {
                  'creation_date': {
                    '$gte': { '$date': extract_start_time },
                    '$lte': { '$date': extract_end_time }
                  }
                },
                {
                  'modified_date': {
                    '$gte': { '$date': extract_start_time },
                    '$lte': { '$date': extract_end_time }
                      }
                    }
              ]
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
      #df.display()
      spark.sql("""
            MERGE INTO dg_performance_management.activity_mapping AS target
            USING activity_mapping AS source
            ON target.orgId = source.orgId
            AND target.campaignId = source.campaignId
            AND target.activityId = source.activityId
            WHEN NOT MATCHED THEN
             INSERT *        
          """)

      