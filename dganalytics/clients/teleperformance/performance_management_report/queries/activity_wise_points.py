from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite


pipeline = [
    {
        "$project": {
            "is_active": 1.0,
            "is_deleted": 1.0,
            "org_id": 1.0,
            "gd_users": "$game_design.gd_users",
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
            "path": "$gd_users",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$lookup": {
            "from": "User",
            "let": {
                    "id": "$gd_users.user_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$eq": [
                                "$_id",
                                "$$id"
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "name": 1.0,
                        "quartile": 1.0,
                        "user_id": 1.0
                    }
                }
            ],
            "as": "userData"
        }
    },
    {
        "$unwind": {
            "path": "$userData",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$unwind": {
            "path": "$outcome",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "outcome": 1.0,
            "quartile": {
                "$filter": {
                    "input": "$outcome.quartile",
                    "as": "q",
                    "cond": {
                        "$eq": [
                            "$$q.quartile",
                            "$userData.quartile"
                        ]
                    }
                }
            },
            "kpi_name": "$outcome.kpi_name",
            "user_mongo_id": "$gd_users.user_id",
            "user_id": "$userData.user_id",
            "org_id": 1.0
        }
    },
    {
        "$unwind": {
            "path": "$quartile",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "user_mongo_id": 1.0,
            "frequency": "$quartile.frequency",
            "activity": "$outcome.name",
            "points_per_activity": "$outcome.quantity",
            "user_id": 1.0,
            "outcome_id": "$outcome._id",
            "outcome_image": "$outcome.outcome_image",
            "org_id": 1.0,
            "kpi_name": 1.0
        }
    },
    {
        "$lookup": {
            "from": "User_Outcome",
            "let": {
                    "user_id": "$user_id",
                    "outcome_id": "$outcome_id"
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
                                },
                                {
                                    "$eq": [
                                        "$outcome_id",
                                        "$$outcome_id"
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            "as": "Outcome"
        }
    },
    {
        "$unwind": {
            "path": "$Outcome",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "_id": 0.0,
            "campaign_id": "$_id",
            "mongo_user_id": "$user_mongo_id",
            "user_id": 1.0,
            "frequency": 1.0,
            "activity_name": "$activity",
            "date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$Outcome.creation_date"
                }
            },
            "kpi_name": 1.0,
            "points": "$Outcome.outcome_quantity",
            "org_id": 1.0
        }
    }
]


schema = StructType([StructField('campaign_id', StructType(
    [StructField('oid', StringType(), True)]), True),
    StructField('activityId', StringType(), True),
    StructField('user_id', StringType(), True),
    StructField('points', IntegerType(), True),
    StructField('outcome_type', StringType(), True),
    StructField('team_id', StringType(), True),
    StructField('kpi_name', StringType(), True),
    StructField('org_id', StringType(), True),
    StructField('field_name', StringType(), True),
    StructField('field_value', DoubleType(), True),
    StructField('frequency', StringType(), True),
    StructField('entity_name', StringType(), True),
    StructField('no_of_times_performed', IntegerType(), True),
    StructField('activity_name', StringType(), True),
    StructField('target', DoubleType(), True),
    StructField('date', StringType(), True),
    StructField('awardedBy', StringType(), True),
    StructField('mongoUserId', StringType(), True)
])

databases = ['holden-prod', 'tp-prod']
def get_activity_wise_points(spark):
    for db in databases:
        df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema, mongodb=db)
        df.registerTempTable("activity_wise_points")
        df = spark.sql("""
                        select  distinct a.campaign_id.oid as campaignId,
                            a.activityId as activityId,
                            a.user_id as userId,
                            a.points as points,
                            a.outcome_type as outcomeType,
                            a.team_id as teamId,
                            a.kpi_name as kpiName,
                            a.field_name as fieldName,
                            a.field_value as fieldValue,
                            a.frequency as frequency,
                            a.entity_name as entityName,
                            a.no_of_times_performed as noOfTimesPerformed,
                            a.activity_name as activityName,
                            a.target as target,
                            cast(a.date as date) as date,
                            a.mongoUserId as mongoUserId,
                            awardedBy as awardedBy,
                            lower(a.org_id) as orgId
                    from activity_wise_points a
                    """)
        '''
        df.write.format("delta").mode("overwrite").partitionBy(
            'orgId').saveAsTable("dg_performance_management.activity_wise_points")
        '''
        delta_table_partition_ovrewrite(
            df, "dg_performance_management.activity_wise_points", ['orgId'])
