from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

def build_pipeline(org_id: str, timezone: str):
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    pipeline = [
        {
            "$match": {
                "org_id": org_id
            }
        }, 
        {
            "$project": {
                "name": 1.0,
                "levels": 1.0,
                "org_id": 1.0
            }
        }, 
        {
            "$unwind": {
                "path": "$levels",
                "preserveNullAndEmptyArrays": False
            }
        }, 
        {
            "$lookup": {
                "from": "User_Levels",
                "let": {
                    "campaignId": "$_id",
                    "levelId": "$levels._id",
                    "orgId": "$org_id"
                },
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {
                                        "$eq": [
                                            "$org_id",
                                            "$$orgId"
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            "$campaign_id",
                                            "$$campaignId"
                                        ]
                                    },
                                    {
                                        "$eq": [
                                            "$level_id",
                                            "$$levelId"
                                        ]
                                    }
                                ]
                            }
                        }
                    }
                ],
                "as": "userLevels"
            }
        }, 
        {
            "$unwind": {
                "path": "$userLevels",
                "preserveNullAndEmptyArrays": False
            }
        }, 
        {
            "$match": {
                "userLevels.achieved_date": {
                    '$gte': { '$date': extract_start_time }
                }
            }
        },

        {
            "$project": {
                "_id": 0.0,
                "mongoUserId": "$userLevels.u_id",
                "userId": "$userLevels.user_id",
                "campaignId": "$_id",
                "levelId": "$userLevels.level_id",
                "achievedDate": {
                    "$toDate": {
                        "$dateToString": {
                            "date": "$userLevels.achieved_date",
                            "timezone": timezone
                        }
                    }
                },
                "campaignName": "$name",
                "levelNumber": "$userLevels.level_no",
                "levelStartPoints": "$levels.start_points",
                "levelEndPoints": "$levels.end_points",
                "orgId": "$org_id"
            }
        }
    ]

    return pipeline

schema = StructType([StructField('achievedDate', TimestampType(), True),
                     StructField('campaignId', StringType(), True),
                     StructField('campaignName', StringType(), True),
                     StructField('levelEndPoints', IntegerType(), True),
                     StructField('levelId', StringType(), True),
                     StructField('levelNumber', IntegerType(), True),
                     StructField('levelStartPoints', IntegerType(), True),
                     StructField('orgId', StringType(), True),
                     StructField('mongoUserId', StringType(), True),
                     StructField('userId', StringType(), True)])


def get_levels(spark):
  for org_timezone in get_active_organization_timezones(spark).rdd.collect():
    org_id = org_timezone['org_id']
    org_timezone = org_timezone['timezone']
    
    pipeline = build_pipeline(org_id, org_timezone)
    
    levels_df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    levels_df = levels_df.withColumn("orgId", lower(levels_df["orgId"]))

    
    levels_df.createOrReplaceTempView("levels")

    spark.sql("""
            MERGE INTO dg_performance_management.levels AS target
            USING levels AS source
            ON target.orgId = source.orgId
            AND target.campaignId = source.campaignId
            AND target.levelId = source.levelId
            AND target.userId = source.userId
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *        
            """)
 
