from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.gpc.gpc_utils import (get_dbname, gpc_request, extract_parser, gpc_utils_logger,
                                                  process_raw_data,
                                                  check_prev_gpc_extract, update_raw_table, get_prev_extract_data)
import traceback
from dganalytics.connectors.gpc.batch.etl.extract_api.conversation_details_job import exec_conversation_details_job
from dganalytics.connectors.gpc.batch.etl.extract_api.evaluation_forms import exec_evaluation_forms_api
from dganalytics.connectors.gpc.batch.etl.extract_api.evaluations import exec_evaluations_api
from dganalytics.connectors.gpc.batch.etl.extract_api.users_details_job import exec_users_details_job_api
from dganalytics.connectors.gpc.batch.etl.extract_api.wfm_adherence import exec_wfm_adherence_api


if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting GPC API {api_name}")

        # if check_prev_gpc_extract(spark, api_name, extract_date, run_id):
        if 1 == 2:
            logger.info(
                f"API {api_name} for {tenant} - {extract_date} already extracted")
            resp_list = get_prev_extract_data(
                tenant, extract_date, run_id, api_name)
            process_raw_data(spark, tenant, api_name, run_id,
                             resp_list, extract_date, 0, re_process=True)
            logger.info("re-processed")
        else:
            if api_name in ["users", "routing_queues", "groups", "users_details", "wrapup_codes",
                            "conversation_details"]:
                df = gpc_request(spark, tenant, api_name, run_id, extract_date)
            elif api_name == "conversation_details_job":
                logger.info("Conversation details job kick off")
                exec_conversation_details_job(
                    spark, tenant, run_id, db_name, extract_date)
            elif api_name == "evaluation_forms":
                logger.info("Evaluation Forms kick off")
                exec_evaluation_forms_api(
                    spark, tenant, run_id, db_name, extract_date)
            elif api_name == "evaluations":
                exec_evaluations_api(spark, tenant, run_id, db_name, extract_date)
            elif api_name == "users_details_job":
                exec_users_details_job_api(
                    spark, tenant, run_id, db_name, extract_date)
            elif api_name == "wfm_adherence":
                exec_wfm_adherence_api(
                    spark, tenant, run_id, db_name, extract_date)
            else:
                logger.exception("invalid api name")

    except Exception as e:
        logger.exception(
            f"Error Occured in GPC Extraction for {extract_date}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise
    finally:
        flush_utils(spark, logger)
