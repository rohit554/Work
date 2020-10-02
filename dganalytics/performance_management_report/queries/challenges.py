from dganalytics.utils.utils import exec_mongo_pipeline

pipeline = [
    {
        "$match": {
        }
    },
    {
        "$project": {
            "org_id": 1.0,
            "name": 1.0,
            "user_id": 1.0,
            "works_for": {
                "$arrayElemAt": [
                    "$works_for",
                    0.0
                ]
            },
            "campaign_challenges": 1.0
        }
    },
    {
        "$match": {
            "works_for.role_id": {
                "$ne": "Team Manager"
            },
            "role_id": {
                "$ne": "Team Manager"
            }
        }
    },
    {
        "$unwind": {
            "path": "$campaign_challenges",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "org_id": 1.0,
            "creation_date_millis": {
                "$add": [
                    {
                        "$toLong": "$campaign_challenges.creation_date_str"
                    },
                    36000000.0
                ]
            },
            "acceptance_date": {
                "$add": [
                    "$campaign_challenges.start_date",
                    36000000.0
                ]
            },
            "completion_date": {
                "$add": [
                    "$campaign_challenges.completion_date",
                    36000000.0
                ]
            },
            "end_date": {
                "$add": [
                    "$campaign_challenges.end_date",
                    36000000.0
                ]
            },
            "name": 1.0,
            "user_id": 1.0,
            "team_id": "$works_for.team_id",
            "role": "$works_for.role",
            "campaign_challenges": 1.0
        }
    },
    {
        "$addFields": {
            "creation_date": {
                "$toDate": "$creation_date_millis"
            }
        }
    },
    {
        "$addFields": {
            "challenges_accepted": {
                "$cond": {
                    "if": {
                        "$eq": [
                            "$campaign_challenges.action",
                            "accepted"
                        ]
                    },
                    "then": 1.0,
                    "else": 0.0
                }
            },
            "challenges_won": {
                "$cond": {
                    "if": {
                        "$and": [
                            {
                                "$eq": [
                                    "$campaign_challenges.status",
                                    "win"
                                ]
                            },
                            {
                                "$eq": [
                                    "$campaign_challenges.status",
                                    "both wins"
                                ]
                            }
                        ]
                    },
                    "then": 1.0,
                    "else": 0.0
                }
            }
        }
    },
    {
        "$lookup": {
            "from": "Campaign",
            "let": {
                    "id": "$campaign_challenges.challenge_id",
                    "org_id": "$org_id"
            },
            "pipeline": [
                {
                    "$project": {
                        "challenge_id": "$$id",
                        "org_id": 1.0,
                        "name": 1.0,
                        "is_active": 1.0,
                        "is_deleted": 1.0,
                        "challenges._id": 1.0,
                        "challenges.name": 1.0,
                        "challenges.frequency": 1.0,
                        "challenges.desc": 1.0,
                        "challenges.no_of_days": 1.0
                    }
                },
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$org_id",
                                        "$$org_id"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$is_active",
                                        True
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$is_deleted",
                                        False
                                    ]
                                },
                                {
                                    "$in": [
                                        "$$id",
                                        "$challenges._id"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "org_id": 1.0,
                        "name": 1.0,
                        "challenges": {
                            "$filter": {
                                "input": "$challenges",
                                "as": "c",
                                "cond": {
                                    "$eq": [
                                        "$$c._id",
                                        "$$id"
                                    ]
                                }
                            }
                        }
                    }
                },
                {
                    "$unwind": {
                        "path": "$challenges",
                        "preserveNullAndEmptyArrays": False
                    }
                },
                {
                    "$project": {
                        "name": 1.0,
                        "challenge_id": "$challenges._id",
                        "challenge_name": "$challenges.name",
                        "challenge_frequency": "$challenges.frequency",
                        "challenge_desc": "$challenges.desc",
                        "no_of_days": "$challenges.no_of_days"
                    }
                }
            ],
            "as": "challenges"
        }
    },
    {
        "$unwind": {
            "path": "$challenges",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "_id": 0.0,
            "org_id": "$org_id",
            "challenger_mongo_id": "$_id",
            "campaign_id": "$challenges._id",
            "challenge_name": "$challenges.challenge_name",
            "challenge_frequency": "$challenges.challenge_frequency",
            "no_of_days": "$challenges.no_of_days",
            "challengee_mongo_id": "$campaign_challenges.user_id",
            "status": "$campaign_challenges.status",
            "action": "$campaign_challenges.action",
            "challenge_thrown_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$creation_date"
                }
            },
            "challenge_acceptance_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$acceptance_date"
                }
            },
            "challenge_end_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$end_date"
                }
            },
            "challenge_completion_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$completion_date"
                }
            }
        }
    }
]


def get_challenges(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'User')
    df.registerTempTable("challenges")
    df = spark.sql("""
                    select  action action,
                            campaign_id.oid campaignId,
                            cast(challenge_thrown_date as date) challengeThrownDate,
                            cast(challenge_acceptance_date as date) challengeAcceptanceDate,
                            cast(challenge_completion_date as date) challengeCompletionDate,
                            cast(challenge_end_date as date) challengeEndDate,
                            challenge_frequency challengeFrequency,
                            challenge_name challengeName,
                            challengee_mongo_id.oid challengeeMongoId,
                            challenger_mongo_id.oid challengerMongoId,
                            no_of_days noOfDays,
                            status status,
                            'salmatcolesonline' orgId
                    from challenges
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.challenges")
