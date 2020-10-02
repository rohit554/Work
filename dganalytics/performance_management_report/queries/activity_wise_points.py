from dganalytics.utils.utils import exec_mongo_pipeline

pipeline = [
    {
        "$project": {
            "is_active": 1.0,
            "is_deleted": 1.0,
            "org_id": 1.0,
            "gd_users": "$game_design.gd_users",
            "outcome": 1
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
            "user_mongo_id": "$gd_users.user_id",
            "user_id": "$userData.user_id"
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
            "outcome_image": "$outcome.outcome_image"
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
            "kpi_name": "$Outcome.kpi_name",
            "points": "$Outcome.outcome_quantity"
        }
    }
]


def get_activity_wise_points(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign')
    df.registerTempTable("activity_wise_points")
    df = spark.sql("""
                    select  activity_name activityName,
                            campaign_id.oid campaignId,
                            cast(date as date) date,
                            frequency frequency,
                            kpi_name kpi_name,
                            mongo_user_id.oid mongoUserId,
                            points points,
                            user_id userId,
                            'salmatcolesonline' orgId
                    from activity_wise_points
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.activity_wise_points")
