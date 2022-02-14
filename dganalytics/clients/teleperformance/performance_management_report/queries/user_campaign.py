from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType

pipeline = [
    {
        "$project": {
            "game_design.gd_users": 1.0,
            "game_design.gd_teams": 1.0,
            "org_id": 1.0
        }
    },
    {
        "$facet": {
            "teams": [
                {
                    "$unwind": {
                        "path": "$game_design.gd_teams",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0.0,
                        "org_id": 1.0,
                        "campaign_id": "$_id",
                        "team_id": "$game_design.gd_teams.team_id"
                    }
                },
                {
                    "$lookup": {
                        "from": "User",
                        "localField": "team_id",
                        "foreignField": "works_for.team_id",
                        "as": "data"
                    }
                },
                {
                    "$unwind": {
                        "path": "$data",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0.0,
                        "user_id": "$data._id",
                        "campaign_id": 1.0,
                        "org_id": 1.0,
                        "team_id": 1.0
                    }
                }
            ],
            "users": [
                {
                    "$unwind": {
                        "path": "$game_design.gd_users",
                        "preserveNullAndEmptyArrays": True
                    }
                },
                {
                    "$project": {
                        "_id": 0.0,
                        "campaign_id": "$_id",
                        "team_id": "$game_design.gd_users.team_id",
                        "org_id": 1.0,
                        "user_id": "$game_design.gd_users.user_id"
                    }
                }
            ]
        }
    },
    {
        "$project": {
            "users": {
                "$concatArrays": [
                    "$users",
                    "$teams"
                ]
            }
        }
    },
    {
        "$unwind": {
            "path": "$users",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$project": {
            "CampaignId": "$users.campaign_id",
            "TeamId": "$users.team_id",
            "UserId": "$users.user_id",
            "OrgId": "$users.org_id"
        }
    }
]

schema = StructType([StructField('CampaignId', StructType([StructField('oid', StringType(), True)]), True),
                     StructField('OrgId', StringType(), True),
                     StructField('TeamId', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('UserId', StructType([StructField('oid', StringType(), True)]), True)])

databases = ['tp-prod']

def get_user_campaign(spark):
    for db in databases:
        df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema, mongodb=db)
        df.createOrReplaceTempView("user_campaign")
        df = spark.sql("""
                        select  distinct CampaignId.oid campaignId,
                                TeamId.oid teamId,
                                UserId.oid userId,
                                lower(OrgId) orgId
                        from user_campaign
                    """)
        '''
        df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
            'orgId').saveAsTable("dg_performance_management.user_campaign")
        '''
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.user_campaign", ['orgId'])
