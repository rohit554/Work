from dganalytics.utils.utils import exec_mongo_pipeline, get_active_organization_timezones
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from datetime import datetime, timedelta

schema = StructType([StructField('answerGiven', StringType(), True),
                     StructField('answeredDate', StringType(), True),
                     StructField('campaignId', StringType(), True),
                     StructField('correctAnswer', StringType(), True),
                     StructField('isCorrect', BooleanType(), True),
                     StructField('question', StringType(), True),
                     StructField('orgId', StringType(), True),
                     StructField('quiz_id', StringType(), True),
                     StructField('subject_area', StringType(), True),
                     StructField('userId', StringType(), True)])


def get_questions(spark):
    extract_start_time = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    for org_timezone in get_active_organization_timezones(spark).rdd.collect():
      org_id = org_timezone['org_id']
      org_timezone = org_timezone['timezone']
      pipeline = [
        {
          "$match":{
              "org_id" : org_id,
              'answered_date': {
                    '$gte': { '$date': extract_start_time }
                }
          }
        },
        {
            "$project": {
                "user_id": 1.0,
                "campaign_id": 1.0,
                "quiz_id": "$questionnaire_id",
                "answered_questions": 1.0,
                "answered_date": 1.0
            }
        },
        {
            "$unwind": {
                "path": "$answered_questions",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$lookup": {
                "from": "questionnaire",
                "localField": "answered_questions.quiz_id",
                "foreignField": "_id",
                "as": "question"
            }
        },
        {
            "$unwind": {
                "path": "$question",
                "preserveNullAndEmptyArrays": True
            }
        },
        {
            "$project": {
                "_id": 0.0,
                "user_id": 1.0,
                "answered_date": "$answered_date",
                "org_id": "$question.org_id",
                "campaign_id": 1.0,
                "quiz_id": 1.0,
                "subject_area": "$question.tag",
                "question": "$question.question",
                "correct_answer": "$question.answer",
                "answer_given": "$answered_questions.answer_given",
                "is_correct": {
                    "$cond": {
                        "if": {
                            "$eq": [
                                "$question.answer",
                                "$answered_questions.answer_given"
                            ]
                        },
                        "then": True,
                        "else": False
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0.0,
                "userId" : "$user_id",
                "answeredDate": {
                    "$dateToString": {
                        "format": "%Y-%m-%d",
                        "date": "$answered_date",
                        "timezone": org_timezone
                    }
                },
                "orgId" : "$org_id",
                "campaignId" : "$campaign_id",
                "quiz_id": 1.0,
                "subject_area": 1.0,
                "question": 1.0,
                "correctAnswer" : "$correct_answer",
                "answerGiven" : "$answer_given",
                "isCorrect" : "$is_correct"
            }
        }
      ]
      df = exec_mongo_pipeline(spark, pipeline, 'quiz', schema)
      df.createOrReplaceTempView("questions")
      df = spark.sql("""
                      select  distinct answerGiven,
                              answeredDate,
                              campaignId,
                              replace(replace(replace(replace(correctAnswer, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') correctAnswer,
                              isCorrect,
                              replace(replace(replace(replace(question, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') question,
                              quiz_id,
                              replace(replace(replace(replace(subject_area, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') subject_area,
                              userId,
                              lower(orgId) orgId
                      from questions
                  """)
      df.createOrReplaceTempView("questions")

      spark.sql("""
            MERGE INTO dg_performance_management.questions AS target
            USING questions AS source
            ON target.orgId = source.orgId
            AND target.campaignId = source.campaignId
            AND target.quiz_id = source.quiz_id
            AND target.userId = source.userId
            AND target.question = source.question
            AND target.subject_area = source.subject_area
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *        
            """)
   