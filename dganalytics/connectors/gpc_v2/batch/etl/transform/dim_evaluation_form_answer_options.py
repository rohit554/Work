from pyspark.sql import SparkSession


def dim_evaluation_form_answer_options(spark: SparkSession, extract_date, extract_start_time, extract_end_time):

    evaluation_form_answer_options = spark.sql("""
        insert overwrite dim_evaluation_form_answer_options
                        select distinct evaluationFormId, questionGroupId, questionId,
                        answerOptions.id as answerOptionId,
    answerOptions.text as answerOptionText, answerOptions.value as answerOptionValue,
    sourceRecordIdentifier, soucePartition  from (
    select evaluationFormId, questionGroupId, questions.id as questionId,
    explode(questions.answerOptions) as answerOptions, sourceRecordIdentifier, soucePartition
    from (
    select evaluationFormId, questionGroups.id as questionGroupId,
    explode(questionGroups.questions) as questions, sourceRecordIdentifier, soucePartition
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups, recordIdentifier as sourceRecordIdentifier,
        concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
            from raw_evaluation_forms
    )))
    """)
