from dganalytics.utils.utils import exec_mongo_pipeline, delta_table_partition_ovrewrite, get_path_vars, get_active_organization_timezones
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
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
  
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
      pipeline = [
      {
          '$match': {
              'org_id': org_timezone['org_id']
          }
      },
      {
          '$project': {
              'name': 1.0, 
              'game_design': 1.0, 
              'questionnaire': 1.0, 
              'org_id': '$org_id',
              'timezone': org_timezone['timezone']
          }
      }, 
      {
          '$unwind': {
              'path': '$questionnaire', 
              'preserveNullAndEmptyArrays': False
          }
      }, 
      {
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
              'timezone': '$timezone', 
              'org_id': '$org_id', 
              'users': '$game_design.gd_users'
          }
      },
      {
          '$addFields': {
              'users': {
                  '$switch': {
                      'branches': [
                          {
                              'case': {
                                  '$ne': [
                                      {
                                          '$ifNull': [
                                              '$quiz_type.user_id', True
                                          ]
                                      }, True
                                  ]
                              }, 
                              'then': '$quiz_user'
                          }, {
                              'case': {
                                  '$eq': [
                                      '$quiz_type.all_users_in_campaign', False
                                  ]
                              }, 
                              'then': {
                                  '$filter': {
                                      'input': '$users', 
                                      'as': 'q', 
                                      'cond': {
                                          '$and': [
                                              {
                                                  '$in': [
                                                      '$$q.team_id', '$quiz_user.team_id'
                                                  ]
                                              }
                                          ]
                                      }
                                  }
                              }
                          }
                      ], 
                      'default': '$users'
                  }
              }
          }
      }, 
      {
          '$project': {
              'quiz_user': 0, 
              'quiz_type': 0
          }
      }, 
      {
          '$unwind': {
              'path': '$users', 
              'preserveNullAndEmptyArrays': False
          }
      }, 
      {
          '$lookup': {
              'from': 'User', 
              'let': {
                  'uid': '$users.user_id'
              }, 
              'pipeline': [
                  {
                      '$match': {
                          '$expr': {
                              '$and': [
                                  {
                                      '$eq': [
                                          '$_id', '$$uid'
                                      ]
                                  }, {
                                      '$in': [
                                          'Agent', '$works_for.role_id'
                                      ]
                                  }
                              ]
                          }
                      }
                  }, {
                      '$project': {
                          'name': 1.0, 
                          'user_id': 1.0
                      }
                  }
              ], 
              'as': 'agent_data'
          }
      }, 
      {
          '$unwind': {
              'path': '$agent_data', 
              'preserveNullAndEmptyArrays': False
          }
      }, 
      {
          '$lookup': {
              'from': 'quiz', 
              'let': {
                  'campaign_id': '$campaign_id', 
                  'uid': '$agent_data.user_id', 
                  'qid': '$quiz_id'
              }, 
              'pipeline': [
                  {
                      '$match': {
                          '$expr': {
                              '$and': [
                                  {
                                      '$eq': [
                                          '$campaign_id', '$$campaign_id'
                                      ]
                                  }, {
                                      '$eq': [
                                          '$user_id', '$$uid'
                                      ]
                                  }, {
                                      '$eq': [
                                          '$questionnaire_id', '$$qid'
                                      ]
                                  }
                              ]
                          }
                      }
                  }
              ], 
              'as': 'quiz_data'
          }
      }, 
      {
          '$unwind': {
              'path': '$quiz_data', 
              'preserveNullAndEmptyArrays': True
          }
      },
      {
          '$match':{
              'quiz_data.answered_date': {
                  '$gte': { '$date': extract_start_time }
              }
          }
      }, 
      {
          '$project': {
              '_id': 0.0, 
              'campaign_id': 1.0, 
              'quizId' : '$quiz_id', 
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
                  '$toDate': {
                      '$dateToString': {
                          'date': '$quiz_data.attempt_start', 
                          'timezone': '$timezone'
                      }
                  }
              }, 
              'quizAttemptEndTime': {
                  '$toDate': {
                      '$dateToString': {
                          'date': '$quiz_data.answered_date', 
                          'timezone': '$timezone'
                      }
                  }
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
      }]
      df = exec_mongo_pipeline(spark, pipeline, 'Campaign', schema)
      df = df.withColumn("orgId", lower(df["orgId"]))
      df.createOrReplaceTempView("quizzes")
      
      spark.sql("""
          MERGE INTO dg_performance_management.quizzes AS target
          USING quizzes AS source
          ON target.orgId = source.orgId
          AND target.userId = source.userId
          AND target.quizId = source.quizId
          AND target.userMongoId = source.userMongoId
          WHEN MATCHED THEN
                UPDATE SET *
          WHEN NOT MATCHED THEN
          INSERT *        
        """)