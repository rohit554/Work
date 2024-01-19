from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.helios.transform.helios_transform import helios_transformation, helios_overwrite_transformation
from dganalytics.helios.helios_utils import transform_parser, helios_utils_logger

if __name__ == "__main__":
    tenant, run_id, extract_date, extract_start_time, extract_end_time, transformation = transform_parser()
    spark = get_spark_session(
        app_name=transformation, tenant=tenant, default_db=f"dgdm_{tenant}")

    logger = helios_utils_logger(tenant, transformation)
    
    try:
        logger.info(f"Applying helios transformation {transformation}")
        if transformation in ["dim_conversations", "fact_conversation_metrics", "dim_conversation_participants", "dim_conversation_sessions", "dim_conversation_session_segments",
                        "dim_surveys", "dim_evaluations", "fact_conversation_evaluations", "fact_conversation_surveys", "fact_transcript_contact_reasons", "fact_transcript_insights", "fact_transcript_actions"]:
            helios_transformation(spark, transformation, tenant, extract_date, extract_start_time, extract_end_time)
        elif transformation in ["dim_users", "dim_queues", "dim_wrap_up_codes"]:
            helios_overwrite_transformation(spark, transformation, tenant)   
        elif transformation in ["dim_conversations_location"]:
            helios_update_transformation(spark, transformation.split("_location")[0], tenant, extract_date, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid transformation name")
            raise Exception
    except Exception as e:
        logger.exception(f"Error Occured in helios transformation for {extract_start_time}_{extract_end_time}_{tenant}_{transformation}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)