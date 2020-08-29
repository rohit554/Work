from pyspark.sql import SparkSession


def dim_evaluation_form_questions(spark: SparkSession, extract_date: str):
    evaluation_form_questions = spark.sql("""
                    insert overwrite dim_evaluation_form_questions
                        select evaluationFormId, questionGroupId, questions.id as questionId, 
    questions.text as questionText,
    questions.commentsRequired as commentsRequired, questions.helpText as helpText,
    questions.isCritical as isCritical, questions.isKill as isKill,
    questions.naEnabled as naEnabled, questions.type as questionType
    from (
    select evaluationFormId, questionGroups.id as questionGroupId,
    explode(questionGroups.questions) as questions
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups from raw_evaluation_forms
    )
    )
                    """)
