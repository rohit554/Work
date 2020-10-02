from dganalytics.utils.utils import exec_mongo_pipeline

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
            "answered_date": {
                "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$answered_date"
                }
            },
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
    }
]


def get_questions(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'quiz')
    df.registerTempTable("questions")
    df = spark.sql("""
                    select  answer_given answerGiven,
                            cast(answered_date as date) answeredDate,
                            campaign_id.oid campaignId,
                            correct_answer correctAnswer,
                            is_correct isCorrect,
                            question question,
                            quiz_id.oid quiz_id,
                            subject_area subject_area,
                            user_id userId,
                            'salmatcolesonline' orgId
                    from questions
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.questions")
