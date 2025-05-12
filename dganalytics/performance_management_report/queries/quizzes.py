from dganalytics.utils.utils import exec_mongo_pipeline, get_path_vars, get_active_organization_timezones
from pyspark.sql.functions import col, to_timestamp, lower
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from datetime import datetime, timedelta

schema = StructType([StructField('answeredDate', StringType(), True),
                     StructField('campaign_id', StringType(), True),
                     StructField('noOfCorrectQuestions',
                                 DoubleType(), True),
                     StructField('orgId', StringType(), True),
                     StructField('quizId', StringType(), True),
                     StructField('quizName', StringType(), True),
                     StructField('quizPercentageScore', DoubleType(), True),
                     StructField('quizStatus', StringType(), True),
                     StructField('teamLeadMongoId', StringType(), True),
                     StructField('totalQuestions', DoubleType(), True),
                     StructField('userId', StringType(), True),
                     StructField('userMongoId', StringType(), True),
                     StructField('quizStartDate', StringType(), True),
                     StructField('quizAttemptStartTime', TimestampType(), True),
                     StructField('quizAttemptEndTime', TimestampType(), True)])


def get_quizzes(spark):
    extract_start_time = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')[:-4] + 'Z'
  
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
      pipeline = [
                {
                    '$match': {
                        'org_id': org_timezone['org_id'], 
                        'is_active': True
                    }
                }, {
                    '$project': {
                        'name': 1.0, 
                        'game_design': 1.0, 
                        'questionnaire': 1.0, 
                        'org_id': '$org_id', 
                        'timezone': org_timezone['timezone']
                    }
                }, {
                    '$unwind': {
                        'path': '$questionnaire', 
                        'preserveNullAndEmptyArrays': False
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$or': [
                                {
                                    '$gte': [
                                        '$questionnaire.start_date',
                                        {
                                            "$dateFromString": {
                                                "dateString": extract_start_time,
                                                "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                            }
                                        }
                                    ]
                                }, {
                                    '$gte': [
                                        '$questionnaire.end_date',
                                        {
                                            "$dateFromString": {
                                                "dateString": extract_start_time,
                                                "format": "%Y-%m-%dT%H:%M:%S.%LZ",
                                            }
                                        }
                                    ]
                                }
                            ]            
                }
                    }
                }, {
                    '$sort': {
                        'questionnaire._id': 1
                    }
                }, {
                    '$project': {
                        'campaign_id': '$_id', 
                        'quiz_id': '$questionnaire._id', 
                        'quiz_name': '$questionnaire.name', 
                        'quiz_start_date': '$questionnaire.start_date', 
                        'quiz_created_by': '$questionnaire.created_by', 
                        'quiz_team_id': '$questionnaire.team_id', 
                        'quiz_user': '$questionnaire.users', 
                        'quiz_type': {
                            '$arrayElemAt': [
                                '$questionnaire.users', 0
                            ]
                        }, 
                        'all_tl': {
                            '$cond': {
                                'if': {
                                    '$in': [
                                        'Team Lead', '$questionnaire.all_option_selected_for'
                                    ]
                                }, 
                                'then': True, 
                                'else': False
                            }
                        }, 
                        'all_agent': {
                            '$cond': {
                                'if': {
                                    '$in': [
                                        'Agent', '$questionnaire.all_option_selected_for'
                                    ]
                                }, 
                                'then': True, 
                                'else': False
                            }
                        }, 
                        'timezone': '$timezone', 
                        'org_id': '$org_id', 
                        'users': '$game_design.gd_users', 
                        'thrown_to': 1
                    }
                }, {
                    '$addFields': {
                        'selected_users': {
                            '$cond': {
                                'if': {
                                    '$gt': [
                                        {
                                            '$size': '$quiz_user'
                                        }, 0
                                    ]
                                }, 
                                'then': {
                                    '$map': {
                                        'input': {
                                            '$filter': {
                                                'input': '$quiz_user', 
                                                'as': 'u', 
                                                'cond': {
                                                    '$or': [
                                                        {
                                                            '$and': [
                                                                {
                                                                    '$eq': [
                                                                        '$$u.type', 'Agent'
                                                                    ]
                                                                }, {
                                                                    '$in': [
                                                                        '$$u.user_id', '$users.user_id'
                                                                    ]
                                                                }
                                                            ]
                                                        }, {
                                                            '$and': [
                                                                {
                                                                    '$eq': [
                                                                        '$$u.type', 'Team Lead'
                                                                    ]
                                                                }, {
                                                                    '$in': [
                                                                        '$$u.user_id', '$users.user_id'
                                                                    ]
                                                                }
                                                            ]
                                                        }, {
                                                            '$and': [
                                                                {
                                                                    '$eq': [
                                                                        '$$u.type', 'Team'
                                                                    ]
                                                                }, {
                                                                    '$in': [
                                                                        '$$u.team_id', '$users.team_id'
                                                                    ]
                                                                }
                                                            ]
                                                        }
                                                    ]
                                                }
                                            }
                                        }, 
                                        'as': 'filteredUser', 
                                        'in': {
                                            'user_id': '$$filteredUser.user_id', 
                                            'team_id': '$$filteredUser.team_id', 
                                            'role': '$$filteredUser.type'
                                        }
                                    }
                                }, 
                                'else': []
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'all_users': {
                            '$switch': {
                                'branches': [
                                    {
                                        'case': {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$all_agent', True
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$all_tl', True
                                                    ]
                                                }
                                            ]
                                        }, 
                                        'then': '$users'
                                    }
                                ], 
                                'default': []
                            }
                        }
                    }
                }, {
                    '$addFields': {
                        'merged_users': {
                            '$concatArrays': [
                                '$selected_users', '$all_users'
                            ]
                        }
                    }
                }, {
                    '$project': {
                        'selected_users': 0, 
                        'all_users': 0
                    }
                }, {
                    '$unwind': {
                        'path': '$merged_users', 
                        'preserveNullAndEmptyArrays': False
                    }
                }, {
                    '$addFields': {
                        'role': {
                            '$ifNull': [
                                '$merged_users.role', None
                            ]
                        }
                    }
                }, {
                    '$addFields': {
                        'user': {
                            '$cond': {
                                'if': {
                                    '$eq': [
                                        '$role', 'Team'
                                    ]
                                }, 
                                'then': {
                                    '$filter': {
                                        'input': '$users', 
                                        'as': 'obj', 
                                        'cond': {
                                            '$eq': [
                                                '$$obj.team_id', '$merged_users.team_id'
                                            ]
                                        }
                                    }
                                }, 
                                'else': '$merged_users'
                            }
                        }
                    }
                }, {
                    '$unwind': {
                        'path': '$user', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$project': {
                        'quiz_user': 0, 
                        'quiz_type': 0, 
                        'users': 0, 
                        'merged_users': 0
                    }
                }, {
                    '$lookup': {
                        'from': 'User', 
                        'localField': 'user.user_id', 
                        'foreignField': '_id', 
                        'pipeline': [
                            {
                                '$match': {
                                    'is_active': True, 
                                    '$expr': {
                                        '$or': [
                                            {
                                                '$in': [
                                                    'Agent', '$works_for.role_id'
                                                ]
                                            }, {
                                                '$in': [
                                                    'Team Lead', '$works_for.role_id'
                                                ]
                                            }
                                        ]
                                    }
                                }
                            }, {
                                '$project': {
                                    'name': 1.0, 
                                    'user_id': 1.0, 
                                    '_id': 1, 
                                    'creation_date': 1, 
                                    'team_history': 1, 
                                    'user_role': {
                                        '$arrayElemAt': [
                                            '$works_for.role_id', 0
                                        ]
                                    }
                                }
                            }
                        ], 
                        'as': 'agent_data'
                    }
                }, {
                    '$unwind': {
                        'path': '$agent_data', 
                        'preserveNullAndEmptyArrays': False
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$or': [
                                {
                                    '$or': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$all_agent', True
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$role', None
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$or': [
                                                {
                                                    '$eq': [
                                                        '$role', '$agent_data.user_role'
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$role', 'Team'
                                                    ]
                                                }
                                            ]
                                        }
                                    ]
                                }, {
                                    '$or': [
                                        {
                                            '$and': [
                                                {
                                                    '$eq': [
                                                        '$all_tl', True
                                                    ]
                                                }, {
                                                    '$eq': [
                                                        '$role', None
                                                    ]
                                                }
                                            ]
                                        }, {
                                            '$eq': [
                                                '$role', '$agent_data.user_role'
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                }, {
                    '$match': {
                        '$expr': {
                            '$gte': [
                                '$agent_data.creation_date', '$quiz_end_date'
                            ]
                        }, 
                        '$or': [
                            {
                                '$expr': {
                                    '$ne': [
                                        {
                                            '$ifNull': [
                                                '$role', ''
                                            ]
                                        }, 'Team'
                                    ]
                                }
                            }, {
                                '$and': [
                                    {
                                        '$expr': {
                                            '$eq': [
                                                {
                                                    '$ifNull': [
                                                        '$role', ''
                                                    ]
                                                }, 'Team'
                                            ]
                                        }
                                    }, {
                                        '$expr': {
                                            '$and': [
                                                {
                                                    '$gte': [
                                                        '$quiz_start_date', '$agent_data.team_history.start_date'
                                                    ]
                                                }, {
                                                    '$or': [
                                                        {
                                                            '$lte': [
                                                                '$quiz_end_date', '$agent_data.team_history.end_date'
                                                            ]
                                                        }, {
                                                            '$eq': [
                                                                '$agent_data.team_history.end_date', None
                                                            ]
                                                        }
                                                    ]
                                                }
												# , {
                                                #     '$eq': [
                                                #         '$team_id', '$agent_data.team_history.team_id'
                                                #     ]
                                                # }
                                            ]
                                        }
                                    }
                                ]
                            }
                        ]
                    }
                }, {
                    '$lookup': {
                        'from': 'quiz', 
                        'localField': 'agent_data.user_id', 
                        'foreignField': 'user_id', 
                        'as': 'quiz_data'
                    }
                }, {
                    '$project': {
                        'campaign_id': 1, 
                        'quiz_id': 1, 
                        'quiz_name': 1, 
                        'quiz_start_date': 1, 
                        'quiz_created_by': 1, 
                        'agent_data': 1,
                        'org_id': 1, 
                        'quiz_data': {
                            '$filter': {
                                'input': '$quiz_data', 
                                'as': 'qd', 
                                'cond': {
                                    '$eq': [
                                        '$$qd.questionnaire_id', '$quiz_id'
                                    ]
                                }
                            }
                        }, 
                        'timezone': 1
                    }
                }, {
                    '$unwind': {
                        'path': '$quiz_data', 
                        'preserveNullAndEmptyArrays': True
                    }
                }, {
                    '$project': {
                        '_id': 0.0, 
                        'campaign_id': 1.0, 
                        'quizId': '$quiz_id', 
                        'name': '$agent_data.name', 
                        'quizName': '$quiz_name', 
                        'userId': '$agent_data.user_id', 
                        'teamLeadMongoId': '$quiz_created_by', 
                        'userMongoId': '$agent_data._id', 
                        'quizStatus': {
                            '$ifNull': [
                                '$quiz_data.quiz_status', 'not_attempted'
                            ]
                        }, 
                        'noOfCorrectQuestions': {
                            '$ifNull': [
                                '$quiz_data.score', 0.0
                            ]
                        }, 
                        'quizStartDate': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$quiz_start_date', 
                                'timezone': '$timezone'
                            }
                        }, 
                        'answeredDate': {
                            '$dateToString': {
                                'format': '%Y-%m-%d', 
                                'date': '$quiz_data.answered_date', 
                                'timezone': '$timezone'
                            }
                        }, 
                        'quizAttemptStartTime': {
                            '$toDate': '$quiz_data.attempt_start'
                        }, 
                        'quizAttemptEndTime': {
                            '$toDate': '$quiz_data.answered_date'
                        }, 
                        'totalQuestions': {
                            '$ifNull': [
                                '$quiz_data.total_questions', 0.0
                            ]
                        }, 
                        'quizPercentageScore': {
                            '$ifNull': [
                                '$quiz_data.percentage_score', 0.0
                            ]
                        }, 
                        'orgId': '$org_id'
                    }   
                }
                ]
      df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
      df = df.withColumn("orgId", lower(df["orgId"]))
      df.createOrReplaceTempView("quizzes")
      spark.sql("""
                DELETE FROM dg_performance_management.quizzes a
                WHERE EXISTS (
                    SELECT 1
                    FROM quizzes source
                    WHERE source.orgId = a.orgId
                    AND source.userId = a.userId
                    AND source.campaign_id = a.campaign_id
                    AND source.quizId = a.quizId
                    AND source.teamLeadMongoId = a.teamLeadMongoId       
                   
                )
         """)     
      spark.sql("""
          MERGE INTO dg_performance_management.quizzes AS target
          USING quizzes AS source
          ON target.orgId = source.orgId
          AND target.userId = source.userId
          AND target.quizId = source.quizId
          AND target.userMongoId = source.userMongoId
          AND target.teamLeadMongoId = source.teamLeadMongoId 
          WHEN NOT MATCHED THEN
          INSERT *        
        """)