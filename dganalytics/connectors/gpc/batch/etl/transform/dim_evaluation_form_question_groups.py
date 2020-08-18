from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import parser, get_dbname

if __name__ == "__main__":
    tenant, run_id, extract_date = parser()
    db_name = get_dbname(tenant)
    spark = get_spark_session(app_name="dim_evaluation_form_question_groups", tenant=tenant, default_db=db_name)

    dim_evaluation_form_question_groups = spark.sql(f"""
				insert overwrite dim_evaluation_form_question_groups
                    select evaluationFormId, questionGroups.id as questionGroupId,
questionGroups.name as questionGroupName, questionGroups.defaultAnswersToHighest as defaultAnswersToHighest,
questionGroups.defaultAnswersToNA as defaultAnswersToNA, questionGroups.manualWeight as manualWeight, 
questionGroups.naEnabled as naEnabled, questionGroups.weight as weight
from (
select id as evaluationFormId, explode(questionGroups) as questionGroups from raw_evaluation_forms 
)
	            """)
