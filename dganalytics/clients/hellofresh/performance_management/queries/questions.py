from dganalytics.utils.utils import exec_mongo_pipeline
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

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

schema = StructType([StructField('answer_given', StringType(), True),
                     StructField('answered_date', StringType(), True),
                     StructField('campaign_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('correct_answer', StringType(), True),
                     StructField('is_correct', BooleanType(), True),
                     StructField('question', StringType(), True),
                     StructField('quiz_id', StructType(
                         [StructField('oid', StringType(), True)]), True),
                     StructField('subject_area', StringType(), True),
                     StructField('user_id', StringType(), True)])


def get_questions(spark):
    df = exec_mongo_pipeline(spark, pipeline, 'quiz', schema, mongodb='hellofresh-prod')
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
                            'hellofresh' orgId
                    from questions
                """)
    df.coalesce(1).write.format("delta").mode("overwrite").partitionBy(
        'orgId').saveAsTable("dg_performance_management.questions")
