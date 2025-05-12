from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_active_org
from pyspark.sql.functions import lower

schema = StructType([StructField('campaignId', StringType(), True),
                    StructField('campaignName', StringType(), True),
                    StructField('activityName', StringType(), True),
                    StructField('activityId', StringType(), True),
                    StructField('isChallengeActivty', BooleanType(), True),
                    StructField('orgId', StringType(), True)])

def get_activity_mapping(spark):
    pipeline = [
        {
          '$match': {
            "org_id" :  {
              '$in' : get_active_org()
            }            
          }
        },
        {
            '$lookup': {
                'from': 'Campaign', 
                'localField': 'campaign_id', 
                'foreignField': '_id', 
                "pipeline": [
                {
                    "$project": {
                        "name": 1
                    }
                }
                ],
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
    delta_table_partition_ovrewrite(df, "dg_performance_management.activity_mapping", ['orgId'])
