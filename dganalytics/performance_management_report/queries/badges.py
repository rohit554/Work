from dganalytics.utils.utils import exec_mongo_pipeline

pipeline = [
    {
        "$match": {
            "outcome_type": "badge"
        }
    },
    {
        "$project": {
            "_id": 0.0,
            "date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$creation_date"
                }
            },
            "campaign_id": 1.0,
            "description": "$badge_desc",
            "badge_name": "$badge_name",
            "lead_mongo_user_id": "$teamlead_id",
            "user_id": 1.0
        }
    }
]

def get_badges(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'User_Outcome')
    df.registerTempTable("badges")
    df = spark.sql("""
                    select  badge_name badgeName,
                            campaign_id.oid campaignId,
                            cast(date as date) date,
                            description description,
                            lead_mongo_user_id.oid leadMongoUserId,
                            user_id userId,
                            'salmatcolesonline' orgId
                    from badges
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.badges")
