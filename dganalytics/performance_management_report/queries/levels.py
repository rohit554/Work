from dganalytics.utils.utils import exec_mongo_pipeline

pipeline = [
    {
        "$match": {

        }
    },
    {
        "$unwind": {
            "path": "$campaign_levels"
        }
    },
    {
        "$project": {
            "user_id": 1.0,
            "campaign_id": "$campaign_levels.campaign_id",
            "level_id": "$campaign_levels.level",
            "achieved_date": "$campaign_levels.achieved_date"
        }
    },
    {
        "$lookup": {
            "from": "Campaign",
            "localField": "campaign_id",
            "foreignField": "_id",
            "as": "campaigns"
        }
    },
    {
        "$unwind": {
            "path": "$campaigns",
            "includeArrayIndex": "arrayIndex",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "user_id": 1.0,
            "campaign_id": 1.0,
            "level_id": 1.0,
            "achieved_date": 1.0,
            "campaign_name": "$campaigns.name",
            "campaign_level": "$campaigns.levels"
        }
    },
    {
        "$unwind": {
            "path": "$campaign_level",
            "includeArrayIndex": "arrayIndex",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$match": {
            "$expr": {
                "$eq": [
                    "$level_id",
                    "$campaign_level._id"
                ]
            }
        }
    },
    {
        "$project": {
            "_id": 0,
            "mongo_user_id": "$_id",
            "user_id": 1.0,
            "campaign_id": 1.0,
            "level_id": 1.0,
            "achieved_date": 1.0,
            "campaign_name": 1.0,
            "level_name": "$campaign_levels.name",
            "level_start_points": "$campaign_level.start_points",
            "level_end_points": "$campaign_level.end_points",
            "level_number": "$campaign_level.id"
        }
    }
]


def get_levels(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'User')
    df.registerTempTable("levels")
    df = spark.sql("""
                    select  achieved_date achievedDate,
                            campaign_id.oid campaignId,
                            campaign_name campaignName,
                            level_end_points levelEndPoints,
                            level_id.oid levelId,
                            level_number levelNumber,
                            level_start_points levelStartPoints,
                            mongo_user_id.oid mongoUserId,
                            user_id userId,
                            'salmatcolesonline' orgId
                    from levels
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.levels")
