from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

pipeline = [
    {
        "$project": {
            "name": 1.0,
            "game_design": 1.0,
            "questionnaire": 1.0,
            "org_id": 1.0
        }
    },

    {
        "$lookup": {
            "from": "Organization",
            "let": {
                "oid": "$org_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$org_id",
                                        "$$oid"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$type",
                                        "Organisation"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "timezone": {
                            "$ifNull": [
                                "$timezone",
                                "Australia/Melbourne"
                            ]
                        }
                    }
                }
            ],
            "as": "org"
        }
    },

    {
        "$unwind": {
            "path": "$org",
            "preserveNullAndEmptyArrays": True
        }
    },

    {
        "$unwind": {
            "path": "$questionnaire",
            "preserveNullAndEmptyArrays": False
        }
    },
    {
        "$project": {
            "campaign_id": "$_id",
            "quiz_id": "$questionnaire._id",
            "quiz_name": "$questionnaire.name",
            "quiz_start_date": "$questionnaire.start_date",
            "quiz_created_by": "$questionnaire.created_by",
            "quiz_team_id": "$questionnaire.team_id",
            "quiz_user": "$questionnaire.users",
            "quiz_type": {"$arrayElemAt": ["$questionnaire.users", 0]},
            "timezone": "$org.timezone",
            "org_id": "$org_id",
            "users": "$game_design.gd_users"
        }
    },
    {
        "$addFields": {
            "users": {
                "$switch": {
                    "branches": [
                        {
                            "case": {
                                "$ne": [
                                    {
                                        "$ifNull": ["$quiz_type.user_id", True]},
                                    True
                                ]
                            },
                            "then": "$quiz_user"
                        },
                        {
                            "case": {
                                "$eq": [
                                    "$quiz_type.all_users_in_campaign",
                                    False
                                ]
                            },
                            "then": {
                                "$filter": {
                                    "input": "$users",
                                    "as": "q",
                                    "cond": {
                                        "$and": [
                                            {
                                                "$in": [
                                                    "$$q.team_id",
                                                    "$quiz_user.team_id"
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                    ],
                    "default": "$users"
                }
            }
        }
    },

    {
        "$project": {
            "quiz_user": 0,
            "quiz_type": 0,
        }
    },

    {
        "$unwind": {
            "path": "$users",
            "preserveNullAndEmptyArrays": False
        }
    },

    {
        "$lookup": {
            "from": "User",
            "let": {
                "uid": "$users.user_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$_id",
                                        "$$uid"
                                    ]
                                },
                                {
                                    "$in": [
                                        "Agent",
                                        "$works_for.role_id"
                                    ]
                                }
                            ]
                        }
                    }
                },
                {
                    "$project": {
                        "name": 1.0,
                        "user_id": 1.0
                    }
                }
            ],
            "as": "agent_data"
        }
    },

    {
        "$unwind": {
            "path": "$agent_data",
            "preserveNullAndEmptyArrays": False
        }
    },

    {
        "$lookup": {
            "from": "quiz",
            "let": {
                "campaign_id": "$campaign_id",
                "uid": "$agent_data.user_id",
                "qid": "$quiz_id"
            },
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {
                                    "$eq": [
                                        "$campaign_id",
                                        "$$campaign_id"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$user_id",
                                        "$$uid"
                                    ]
                                },
                                {
                                    "$eq": [
                                        "$questionnaire_id",
                                        "$$qid"
                                    ]
                                }
                            ]
                        }
                    }
                }
            ],
            "as": "quiz_data"
        }
    },
    {
        "$unwind": {
            "path": "$quiz_data",
            "preserveNullAndEmptyArrays": True
        }
    },
    {
        "$project": {
            "_id": 0.0,
            "campaign_id": 1.0,
            "quiz_id": 1.0,
            "quiz_name": 1.0,
            "user_id": "$agent_data.user_id",
            "team_lead_mongo_id": "$quiz_created_by",
            "user_mongo_id": "$agent_data._id",
            "quiz_status": {
                "$ifNull": [
                    "$quiz_data.quiz_status",
                    "not_attempted"
                ]
            },
            "no_of_correct_questions": {
                "$ifNull": [
                    "$quiz_data.score",
                    0.0
                ]
            },
            "quiz_start_date":{
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$toDate": {
                            "$dateToString": {
                                "date": "$quiz_start_date",
                                "timezone": "$timezone"
                            }
                        }
                    }
                }
            },
            "answered_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$toDate": {
                            "$dateToString": {
                                "date": "$quiz_data.answered_date",
                                "timezone": "$timezone"
                            }
                        }
                    }
                }
            },
            'quiz_attempt_start_time': {
                '$toDate': {
                            '$dateToString': {
                                'date': '$quiz_data.attempt_start', 
                                'timezone': '$timezone'
                            }
                        } 
            }, 
            'quiz_attempt_end_time': {
                        '$toDate': {
                            '$dateToString': {
                                'date': '$quiz_data.answered_date', 
                                'timezone': '$timezone'
                            }
                        } 
            },
            "total_questions": {
                "$ifNull": [
                    "$quiz_data.total_questions",
                    0.0
                ]
            },
            "quiz_percentage_score": {
                "$ifNull": [
                    "$quiz_data.percentage_score",
                    0.0
                ]
            },
            "org_id": "$org_id"
        }
    }
]

schema = StructType([StructField('answered_date', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('no_of_correct_questions',
                                 DoubleType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('quiz_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('quiz_name', StringType(), True),
                     StructField('quiz_percentage_score', DoubleType(), True),
                     StructField('quiz_status', StringType(), True),
                     StructField('team_lead_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('total_questions', DoubleType(), True),
                     StructField('user_id', StringType(), True),
                     StructField('user_mongo_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('quiz_start_date', StringType(), True),
                     StructField('quiz_attempt_start_time', TimestampType(), True),
                     StructField('quiz_attempt_end_time', TimestampType(), True)])


def get_quizzes(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
    df.createOrReplaceTempView("quizzes")
    df = spark.sql("""
                    select  distinct cast(answered_date as date) answeredDate,
                            campaign_id.oid campaign_id,
                            cast(no_of_correct_questions as int) noOfCorrectQuestions,
                            quiz_id.oid quizId,
                            quiz_name quizName,
                            cast(quiz_percentage_score as float) quizPercentageScore,
                            quiz_status quizStatus,
                            team_lead_mongo_id.oid teamLeadMongoId,
                            cast(total_questions as int) totalQuestions,
                            user_id userId,
                            user_mongo_id.oid userMongoId,
                            lower(org_id) orgId,
                            cast(quiz_start_date as date) quizStartDate,
                            quiz_attempt_start_time quizAttemptStartTime,
                            quiz_attempt_end_time quizAttemptEndTime
                    from quizzes
                """)
    delta_table_partition_ovrewrite(
        df, "dg_performance_management.quizzes", ['orgId'])