from dganalytics.utils.utils import get_spark_session, flush_utils
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
    "fact_wfm_day_metrics": transform.fact_wfm_day_metrics,
    "fact_wfm_exceptions": transform.fact_wfm_exceptions,
    "dim_wrapup_codes": transform.dim_wrapup_codes,
    "fact_wfm_forecast": transform.fact_wfm_forecast,
    "fact_conversation_aggregate_metrics": transform.fact_conversation_aggregate_metrics,
    "fact_user_presence": transform.fact_user_presence,
    "dim_last_handled_conversation": transform.dim_last_handled_conversation,
    "fact_conversation_transcript_topics": transform.fact_conversation_transcript_topics,
    "fact_conversation_transcript_sentiments": transform.fact_conversation_transcript_sentiments,
    "fact_conversation_transcript_phrases": transform.fact_conversation_transcript_phrases,
    "fact_speechandtextanalytics": transform.fact_speechandtextanalytics
}

if __name__ == "__main__":
    tenant, run_id, extract_date, extract_start_time, extract_end_time, transformation = transform_parser()
    spark = get_spark_session(
        app_name=transformation, tenant=tenant, default_db=get_dbname(tenant))

    logger = gpc_utils_logger(tenant, transformation)
    try:
        logger.info(f"Applying transformation {transformation}")
        transform_to_method[transformation](spark, extract_date, extract_start_time, extract_end_time)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)
