from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql.functions import lower

def build_pipeline(org_id: str, timezone: str):
    extract_start_time = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'
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
                "localField": "org_id",
                "foreignField": "org_id",
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
                "$expr": {
                    "$and": [
                        { "$eq": ["$userLevels.campaign_id", "$_id"] },
                        { "$eq": ["$userLevels.level_id", "$levels._id"] },
                        { "$gte": ["$userLevels.achieved_date", { "$dateFromString": { "dateString": extract_start_time, "format": "%Y-%m-%dT%H:%M:%S.%LZ" } }] }
                    ]
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
                       "$toDate": "$userLevels.achieved_date" 
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
                DELETE FROM dg_performance_management.levels target
                WHERE EXISTS (
                    SELECT 1
                    FROM levels b
                    WHERE b.userId = target.userId
                    AND b.campaignId = target.campaignId
                    AND b.orgId = target.orgId
                    AND b.levelId = target.levelId
                )
    """)
    spark.sql("""
                INSERT INTO dg_performance_management.levels
                    SELECT * FROM levels
    """)
    # spark.sql("""
    #         MERGE INTO dg_performance_management.levels AS target
    #         USING levels AS source
    #         ON target.orgId = source.orgId
    #         AND target.campaignId = source.campaignId
    #         AND target.levelId = source.levelId
    #         AND target.userId = source.userId
    #         WHEN MATCHED THEN
    #             UPDATE SET *
    #         WHEN NOT MATCHED THEN
    #             INSERT *        
    #         """)
 
