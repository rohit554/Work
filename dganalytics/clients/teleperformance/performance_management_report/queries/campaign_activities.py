from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

pipeline = [
    {
        "$project": {
            "is_active": 1.0,
            "is_deleted": 1.0,
            "org_id": 1.0,
            "outcome": 1.0
        }
    },
    {
        "$match": {
            "is_active": True,
            "is_deleted": False
        }
    },
    {
        "$unwind": {
            "path": "$outcome",
            "includeArrayIndex": "arrayIndex",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "_id": 0,
            "CampaignId": "$_id",
            "ActivityId": "$outcome._id",
            "ActivityName": "$outcome.name",
            "KpiName": "$outcome.kpi_name",
            "IsChallengeActivity": "$outcome.challenge_flag",
            "OrgId": "$org_id"
        }
    }
]

schema = StructType([StructField('CampaignId', StringType(), True),
                     StructField('ActivityId', StringType(), True),
                     StructField('ActivityName', StringType(), True),
                     StructField('KpiName', StringType(), True),
                     StructField('IsChallengeActivity', StringType(), True),
                     StructField('OrgId', StringType(), True)
                     ])

databases = ['tp-prod']

def get_campaign_activities(spark):
    for db in databases:
        df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema, mongodb=db)
        df.registerTempTable("campaign_activities")
        df = spark.sql("""
                        select  campaignId string,
                                activityId string,
                                ActivityName as activityName,
                                KpiName as kpiName,
                                IsChallengeActivity as isChallengeActivity,
                                lower(OrgId) as orgId
                        from campaign_activities
                    """)
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.campaign_activities", ['orgId'])
