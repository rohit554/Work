from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

def build_pipeline(org_id: str, org_timezone: str):
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
        "$project": {
            "_id": 0.0,
            "mongo_user_id": "$userLevels.u_id",
            "user_id": "$userLevels.user_id",
            "campaign_id": "$_id",
            "level_id": "$userLevels.level_id",
            "achieved_date": {
                "$toDate": {
                    "$dateToString": {
                        "date": "$userLevels.achieved_date",
                        "timezone": org_timezone
                    }
                }
            },
            "campaign_name": "$name",
            "level_name": "$levels.name",
            "level_number": "$userLevels.level_no",
            "level_start_points": "$levels.start_points",
            "level_end_points": "$levels.end_points",
            "org_id": 1.0
        }
    }
]

    return pipeline

schema = StructType([StructField('achieved_date', TimestampType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('campaign_name', StringType(), True),
                     StructField('level_end_points', IntegerType(), True),
                     StructField('level_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('level_number', IntegerType(), True),
                     StructField('level_start_points', IntegerType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('mongo_user_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('user_id', StringType(), True)])

org_timezone_schema = StructType([
    StructField('org_id', StringType(), True),
    StructField('timezone', StringType(), False)])

def get_levels(spark):
    org_timezone_pipeline = [{
        "$match": {
            "$expr": {
                "$and": [
                {
                    "$eq": ["$type", "Organisation"]
                }, {
                    "$eq": ["$is_active", True]
                }, {
                    "$eq": ["$is_deleted", False]
                }]
            }
        }
    }, {
        "$project": {
            "org_id": 1,
            "timezone": {
                "$ifNull": ["$timezone", "Australia/Melbourne"]
            }
        }
    }]

    org_id_rows = exec_mongo_pipeline(
        spark, org_timezone_pipeline, 'Organization',
        org_timezone_schema).select("*").collect()

    df = None

    # Get campaign & challenges for each org

    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['org_id']
        org_timezone = org_id_row.asDict()['timezone']

        pipeline = build_pipeline(org_id, org_timezone)
        
        levels_df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)

        if df is None:
            df = levels_df
        else:
            df = df.union(levels_df)

    df.createOrReplaceTempView("levels")
    df = spark.sql("""
                    select  distinct achieved_date achievedDate,
                            campaign_id.oid campaignId,
                            campaign_name campaignName,
                            level_end_points levelEndPoints,
                            level_id.oid levelId,
                            level_number levelNumber,
                            level_start_points levelStartPoints,
                            mongo_user_id.oid mongoUserId,
                            user_id userId,
                            lower(org_id) orgId
                    from levels
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.levels")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.levels", ['orgId'])
