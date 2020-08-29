from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, extract_parser, gpc_utils_logger
from dganalytics.connectors.gpc.batch.etl import extract_api
import traceback

if __name__ == "__main__":
    tenant, run_id, extract_date, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(
        app_name=app_name, tenant=tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting GPC API {api_name}")
        logger.info(dir(extract_api))

        if api_name in ["users", "routing_queues", "groups", "users_details", "wrapup_codes",
                        "conversation_details"]:
            df = gpc_request(spark, tenant, api_name, run_id, extract_date)
        elif api_name == "conversation_details_job":
            extract_api.conversation_details_job(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "evaluation_forms":
            extract_api.exec_evaluation_forms_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "evaluations":
            extract_api.exec_evaluations_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "users_details_job":
            extract_api.exec_users_details_job_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "wfm_adherence":
            extract_api.exec_wfm_adherence_api(spark, tenant, run_id, db_name, extract_date)
        else:
            logger.exception("invalid api name")

    except Exception as e:
        traceback.print_exc()
        logger.exception(str(e))
