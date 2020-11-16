from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.sdx.sdx_utils import transform_parser, get_dbname, sdx_utils_logger
from dganalytics.connectors.sdx.batch.etl import transform

transform_to_method = {
    "dim_surveys": transform.dim_surveys,
    "dim_questions": transform.dim_questions,
    "dim_hellofresh_interactions": transform.dim_hellofresh_interactions,
}


if __name__ == "__main__":
    tenant, run_id, extract_date, extract_start_time, extract_end_time, transformation = transform_parser()
    spark = get_spark_session(
        app_name=transformation, tenant=tenant, default_db=get_dbname(tenant))

    logger = sdx_utils_logger(tenant, transformation)
    try:
        logger.info(f"Applying transformation {transformation}")
        transform_to_method[transformation](spark, extract_date, extract_start_time, extract_end_time, tenant)
    except Exception as e:
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)
