from dganalytics.utils.utils import delta_table_partition_ovrewrite
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from dganalytics.clients.breadfinancial.utils import exec_mongo_pipeline

pipeline = [
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
        "$project": {
            "_id": 0.0,
            "user_id": 1.0,
            "answered_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": {
                        "$toDate": {
                            "$dateToString": {
                                "date": "$answered_date",
                                "timezone": "$org.timezone"
                            }
                        }
                    }
                }
            },
            "org_id": 1.0,
            "campaign_id": 1.0,
            "quiz_id": 1.0,
            "subject_area": 1.0,
            "question": 1.0,
            "correct_answer": 1.0,
            "answer_given": 1.0,
            "is_correct": 1.0
        }
    }
]

schema = StructType([StructField('answer_given', StringType(), True),
                     StructField('answered_date', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('correct_answer', StringType(), True),
                     StructField('is_correct', BooleanType(), True),
                     StructField('question', StringType(), True),
                     StructField('org_id', StringType(), True),
                     StructField('quiz_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('subject_area', StringType(), True),
                     StructField('user_id', StringType(), True)])


def get_questions(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'quiz', schema)
    df.createOrReplaceTempView("questions")
    df = spark.sql("""
                    select  distinct answer_given answerGiven,
                            cast(answered_date as date) answeredDate,
                            campaign_id.oid campaignId,
                            replace(replace(replace(replace(correct_answer, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') correctAnswer,
                            is_correct isCorrect,
                            replace(replace(replace(replace(question, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') question,
                            quiz_id.oid quiz_id,
                            replace(replace(replace(replace(subject_area, '\\n', ' '), '""',''), '\\r', ' ') , '\\r\\n', ' ') subject_area,
                            user_id userId,
                            lower(org_id) orgId
                    from questions
                """)
    '''
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.questions")
    '''
    delta_table_partition_ovrewrite(df, "dg_performance_management.questions", ['orgId'])