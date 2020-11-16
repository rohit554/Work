from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.sdx.sdx_utils import get_dbname, sdx_request, extract_parser, sdx_utils_logger
from dganalytics.connectors.sdx.batch.etl.extract_api.interactions import exec_interactions
from dganalytics.connectors.sdx.batch.etl.extract_api.interactions_history import exec_interactions_history

if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "sdx_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = sdx_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting Survey Dynamix API {api_name}")

        if api_name in ["questions", "surveys"]:
            df = sdx_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name in ["interactions"]:
            df = exec_interactions(spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name in ["interactions_history"]:
            df = exec_interactions_history(spark, tenant, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid api name")
            raise Exception

    except Exception as e:
        logger.exception(
            f"Error Occured in Survey Dynamix Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)
