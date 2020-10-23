from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

pipeline = [
    {
        "$match": {
            "outcome_type": "badge"
        }
    },
    {
        "$lookup": {
            "from": "User",
            "let": {
                    "user_id": "$user_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$user_id",
                                        "$$user_id"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "org_id": 1.0
                    }
                }
            ],
            "as": "users"
        }
    },
    {
        "$unwind": {
            "path": "$users",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "_id": 0,
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
            "user_id": 1.0,
            "org_id": "$users.org_id"
        }
    }
]

schema = StructType([StructField('badge_name', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('date', StringType(), True),
                     StructField('description', StringType(), True),
                     StructField('lead_mongo_user_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('user_id', StringType(), True),
                     StructField('org_id', StringType(), True)])

databases = ['holden-prod', 'tp-prod']

def get_badges(spark):
    for db in databases:
        df = exec_mongo_pipeline(spark, pipeline, 'User_Outcome', schema, mongodb=db)
        df.registerTempTable("badges")
        df = spark.sql("""
                        select  badge_name badgeName,
                                campaign_id.oid campaignId,
                                cast(date as date) date,
                                replace(replace(replace(replace(description, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') description,
                                lead_mongo_user_id.oid leadMongoUserId,
                                user_id userId,
                                lower(org_id) orgId
                        from badges
                    """)
        '''
        df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
            'orgId').saveAsTable("dg_performance_management.badges")
        '''
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.badges", ['orgId'])
