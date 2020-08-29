from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import transform_parser, get_dbname, gpc_utils_logger
import traceback
from dganalytics.connectors.gpc.batch.etl import transform

transform_to_method = {
    "dim_conversations": transform.dim_conversations,
    "dim_evaluation_form_answer_options": transform.dim_evaluation_form_answer_options,
    "dim_evaluation_form_question_groups": transform.dim_evaluation_form_question_groups,
    "dim_evaluation_form_questions": transform.dim_evaluation_form_questions,
    "dim_evaluation_forms": transform.dim_evaluation_forms,
    "dim_evaluations": transform.dim_evaluations,
    "dim_managers": transform.dim_managers,
    "dim_routing_queues": transform.dim_routing_queues,
    "dim_user_groups": transform.dim_user_groups,
    "dim_users": transform.dim_users,
    "fact_conversation_metrics": transform.fact_conversation_metrics,
    "fact_evaluation_question_group_scores": transform.fact_evaluation_question_group_scores,
    "fact_evaluation_question_scores": transform.fact_evaluation_question_scores,
    "fact_evaluation_total_scores": transform.fact_evaluation_total_scores,
    "fact_primary_presence": transform.fact_primary_presence,
    "fact_routing_status": transform.fact_routing_status,
    "fact_wfm_actuals": transform.fact_wfm_actuals,
    "fact_wfm_day_metrics": transform.fact_wfm_day_metrics
}


if __name__ == "__main__":
    tenant, run_id, extract_date, transformation = transform_parser()
    spark = get_spark_session(
        app_name=transformation, tenant=tenant, default_db=get_dbname(tenant))

    logger = gpc_utils_logger(tenant, transformation)
    try:
        logger.info(f"Applying transformation {transformation}")
        transform_to_method[transformation](spark, extract_date)
    except Exception as e:
        logger.exception(traceback.print_exc())
