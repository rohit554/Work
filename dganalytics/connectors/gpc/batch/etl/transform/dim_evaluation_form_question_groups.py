from pyspark.sql import SparkSession


def dim_evaluation_form_question_groups(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    evaluation_form_question_groups = spark.sql("""
                    insert overwrite dim_evaluation_form_question_groups
                        select distinct evaluationFormId, questionGroups.id as questionGroupId,
    questionGroups.name as questionGroupName, questionGroups.defaultAnswersToHighest as defaultAnswersToHighest,
    questionGroups.defaultAnswersToNA as defaultAnswersToNA, questionGroups.manualWeight as manualWeight,
    questionGroups.naEnabled as naEnabled, questionGroups.weight as weight, sourceRecordIdentifier, soucePartition
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups,recordIdentifier as sourceRecordIdentifier,
concat(extractDate, '|', extractIntervalStartTime, '|', extractIntervalEndTime) as soucePartition
 from raw_evaluation_forms
    )
                    """)
