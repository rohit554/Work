from pyspark.sql import SparkSession

def dim_evaluation_form_answer_options(spark: SparkSession, extract_date: str):

    evaluation_form_answer_options = spark.sql("""
        insert overwrite dim_evaluation_form_answer_options
                        select evaluationFormId, questionGroupId, questionId, answerOptions.id as answerOptionId,
    answerOptions.text as answerOptionText, answerOptions.value as answerOptionValue  from (
    select evaluationFormId, questionGroupId, questions.id as questionId,
    explode(questions.answerOptions) as answerOptions
    from (
    select evaluationFormId, questionGroups.id as questionGroupId,
    explode(questionGroups.questions) as questions
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups from raw_evaluation_forms
    )))
    """)
