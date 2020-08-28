from pyspark import sql
from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname, gpc_utils_logger

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    db_name = get_dbname(tenant)
    app_name = "dim_evaluation_form_answer_options"
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)

    logger = gpc_utils_logger(tenant, app_name)
    try:
        logger.info("Overwriting into dim_evaluation_form_answer_options")
        dim_evaluation_form_answer_options = spark.sql("""
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

    except Exception as e:
        logger.error(str(e))
