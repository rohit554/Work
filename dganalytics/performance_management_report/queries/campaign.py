from dganalytics.utils.utils import exec_mongo_pipeline

pipeline = [
    {
        "$project": {
            "_id": 0.0,
            "campaign_id": "$_id",
            "name": 1.0,
            "start_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$start_date"
                }
            },
            "end_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$end_date"
                }
            },
            "org_id": 1.0,
            "is_active": 1.0,
            "is_deleted": 1.0
        }
    }
]


def get_campaign(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign')
    df.registerTempTable("campaign")
    df = spark.sql("""
                    select  campaign_id.oid campaignId,
                            cast(start_date as date) start_date,
                            cast(end_date as date) endDate,
                            is_active isActive,
                            is_deleted isDeleted,
                            name name,
                            'salmatcolesonline' orgId
                    from campaign
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.campaign")
