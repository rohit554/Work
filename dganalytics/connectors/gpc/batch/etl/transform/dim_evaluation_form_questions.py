from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = transform_parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="dim_evaluation_form_questions", tenant=tenant, default_db=db_name)

    dim_evaluation_form_questions = spark.sql(f"""
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
