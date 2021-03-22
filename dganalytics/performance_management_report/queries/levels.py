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
                "$unwind" : { 
                    "path" : "$campaign_levels"
                }
            }, 
            { 
                "$project" : { 
                    "user_id" : 1.0, 
                    "campaign_id" : "$campaign_levels.campaign_id", 
                    "level_id" : "$campaign_levels.level", 
                    "achieved_date" : "$campaign_levels.achieved_date", 
                    "org_id" : 1.0
                }
            }, 
            { 
                "$lookup" : { 
                    "from" : "Campaign", 
                    "localField" : "campaign_id", 
                    "foreignField" : "_id", 
                    "as" : "campaigns"
                }
            }, 
            { 
                "$unwind" : { 
                    "path" : "$campaigns", 
                    "includeArrayIndex" : "arrayIndex", 
                    "preserveNullAndEmptyArrays" : False
                }
            }, 
            { 
                "$project" : { 
                    "user_id" : 1.0, 
                    "campaign_id" : 1.0, 
                    "level_id" : 1.0, 
                    "achieved_date" : 1.0, 
                    "campaign_name" : "$campaigns.name", 
                    "campaign_level" : "$campaigns.levels", 
                    "org_id" : 1.0
                }
            }, 
            { 
                "$unwind" : { 
                    "path" : "$campaign_level", 
                    "includeArrayIndex" : "arrayIndex", 
                    "preserveNullAndEmptyArrays" : False
                }
            }, 
            { 
                "$match" : { 
                    "$expr" : { 
                        "$eq" : [
                            "$level_id", 
                            "$campaign_level._id"
                        ]
                    }
                }
            }, 
            { 
                "$project" : { 
                    "_id" : 0.0, 
                    "mongo_user_id" : "$_id", 
                    "user_id" : 1.0, 
                    "campaign_id" : 1.0, 
                    "level_id" : 1.0, 
                    "achieved_date" : 1.0, 
                    "campaign_name" : 1.0, 
                    "level_name" : "$campaign_levels.name", 
                    "level_start_points" : "$campaign_level.start_points", 
                    "level_end_points" : "$campaign_level.end_points", 
                    "level_number" : "$campaign_level.id", 
                    "org_id" : 1.0
                }
            },
            { 
                "$project" : { 
                    "mongo_user_id" : 1.0, 
                    "user_id" : 1.0, 
                    "campaign_id" : 1.0, 
                    "level_id" : 1.0, 
                    "achieved_date" : { 
                        "$toDate" : { 
                            "$dateToString" : { 
                                "date" : "$achieved_date", 
                                "timezone" : org_timezone
                            }
                        }
                    }, 
                    "campaign_name" : 1.0, 
                    "level_name" : 1.0, 
                    "level_start_points" : 1.0, 
                    "level_end_points" : 1.0, 
                    "level_number" : 1.0, 
                    "org_id" : 1.0
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
        
        levels_df = exec_mongo_pipeline(spark, pipeline, 'User', schema)

        if df is None:
            df = levels_df
        else:
            df = df.union(levels_df)

    df.registerTempTable("levels")
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
