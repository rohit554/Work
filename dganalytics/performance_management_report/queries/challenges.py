from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def build_pipeline(org_id: str, org_timezone: str):     
    pipeline = [
        {
            "$match": {
                "$expr": {
                    "$eq": [{"$strcasecmp": ["$org_id", org_id]}, 0]
                },
                "works_for.role_id": {
                    "$ne": "Team Manager"
                },
                "role_id": {
                    "$ne": "Team Manager"
                }
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
                "challenge_thrown_date": "$creation_date",
                "challenge_acceptance_date": "$acceptance_date",
                "challenge_end_date": "$end_date",
                "challenge_completion_date": "$completion_date"
            }
        },
        {
            "$project": {
                "org_id": "$org_id",
                "challenger_mongo_id": 1.0,
                "campaign_id": 1.0,
                "challenge_name": 1.0,
                "challenge_frequency": 1.0,
                "no_of_days": 1.0,
                "challengee_mongo_id": 1.0,
                "status": 1.0,
                "action": 1.0,
                "challenge_end_date": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$challenge_end_date", 
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {
                                    "$toDate": {
                                        "$dateToString": {
                                            "date": "$challenge_end_date",
                                            "timezone": org_timezone
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "challenge_thrown_date": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": {
                            "$toDate": {
                                "$dateToString": {
                                    "date": "$challenge_thrown_date",
                                    "timezone": org_timezone
                                }
                            }
                        }
                    }
                },
                "challenge_acceptance_date": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$challenge_acceptance_date", 
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {
                                    "$toDate": {
                                        "$dateToString": {
                                            "date": "$challenge_acceptance_date",
                                            "timezone": org_timezone
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "challenge_completion_date": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$challenge_completion_date", 
                                        None
                                    ]
                                }
                            ]
                        },
                        "then": None,
                        "else": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {
                                    "$toDate": {
                                        "$dateToString": {
                                            "date": "$challenge_completion_date",
                                            "timezone": org_timezone
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    ]
    return pipeline

schema = StructType([StructField('action', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('challenge_acceptance_date',
                                 StringType(), True),
                     StructField('challenge_completion_date',
                                 StringType(), True),
                     StructField('challenge_end_date', StringType(), True),
                     StructField('challenge_frequency', IntegerType(), True),
                     StructField('challenge_name', StringType(), True),
                     StructField('challenge_thrown_date', StringType(), True),
                     StructField('challengee_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('challenger_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('no_of_days', IntegerType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('status', StringType(), True)])

org_timezone_schema = StructType([
        StructField('timezone', StringType(), False)
])

def get_challenges(spark):
    org_id_rows = spark.sql(
        "select distinct orgId from dg_performance_management.campaign order by orgId"
    ).select("orgId").collect()
    
    df = None

    # Get campaign & challenges for each org
    for org_id_row in org_id_rows:
        org_id = org_id_row.asDict()['orgId']

        org_timezone_pipeline = [{
            "$match": {
                "$expr": {
                    "$and": [{
                        "$eq": [{
                            "$strcasecmp": ["$org_id", org_id]
                        }, 0]
                    }, {
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
                "org_id": 1.0,
                "timezone": {
                    "$ifNull": ["$timezone", "Australia/Melbourne"]
                }
            }
        }]

        org_timezone_row = exec_mongo_pipeline(
            spark, org_timezone_pipeline, 'Organization',
            org_timezone_schema).select("timezone").first()

        if not org_timezone_row:
            continue
        
        org_timezone = org_timezone_row.asDict()['timezone']
        pipeline = build_pipeline(org_id, org_timezone)

        challenges_df = exec_mongo_pipeline(spark, pipeline, 'User', schema)
        
        if df is None:
            df = challenges_df
        else:
            df = df.union(challenges_df)
    
    df.registerTempTable("challenges")
    df = spark.sql("""
                    select  distinct action action,
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
                            lower(org_id) orgId
                    from challenges
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.challenges")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.challenges", ['orgId'])
