from pyspark.sql import SparkSession


def dim_evaluation_form_questions(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_form_questions = spark.sql("""
                    insert overwrite dim_evaluation_form_questions
                        select distinct evaluationFormId, questionGroupId, questions.id as questionId, 
    questions.text as questionText,
    questions.commentsRequired as commentsRequired, questions.helpText as helpText,
    questions.isCritical as isCritical, questions.isKill as isKill,
    questions.naEnabled as naEnabled, questions.type as questionType, sourceRecordIdentifier, soucePartition
    from (
    select evaluationFormId, questionGroups.id as questionGroupId,
    explode(questionGroups.questions) as questions, sourceRecordIdentifier, soucePartition
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups,recordIdentifier as sourceRecordIdentifier,
concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
 from raw_evaluation_forms
    )
    )
                    """)
