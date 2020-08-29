from pyspark.sql import SparkSession


def dim_evaluation_form_question_groups(spark: SparkSession, extract_date: str):
    evaluation_form_question_groups = spark.sql("""
                    insert overwrite dim_evaluation_form_question_groups
                        select evaluationFormId, questionGroups.id as questionGroupId,
    questionGroups.name as questionGroupName, questionGroups.defaultAnswersToHighest as defaultAnswersToHighest,
    questionGroups.defaultAnswersToNA as defaultAnswersToNA, questionGroups.manualWeight as manualWeight,
    questionGroups.naEnabled as naEnabled, questionGroups.weight as weight
    from (
    select id as evaluationFormId, explode(questionGroups) as questionGroups from raw_evaluation_forms
    )
                    """)
