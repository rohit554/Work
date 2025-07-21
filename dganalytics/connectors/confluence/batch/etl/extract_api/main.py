from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.confluence.confluence_utils import get_dbname, confluence_request, extract_parser, confluence_utils_logger
from dganalytics.connectors.confluence.batch.etl.extract_api.page_index import exec_confluence_page_index
from dganalytics.connectors.confluence.batch.etl.extract_api.page_body import exec_confluence_page_details_job


if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "confluence_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = confluence_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting confluence API {api_name}")

        if api_name == "page_index":
            logger.info("page_index job kick off")
            exec_confluence_page_index(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "page_body":
            logger.info("page_body job kick off")
            exec_confluence_page_details_job(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid api name")
            raise Exception

    except Exception as e:
        logger.exception(
            f"Error Occured in confluence Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
