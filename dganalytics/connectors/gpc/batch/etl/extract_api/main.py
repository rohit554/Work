from dganalytics.utils.utils import get_spark_session
from dganalytics.connectors.gpc.gpc_utils import get_dbname, gpc_request, extract_parser, gpc_utils_logger
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

        if api_name in ["users", "routing_queues", "groups", "users_details", "wrapup_codes",
                        "conversation_details"]:
            df = gpc_request(spark, tenant, api_name, run_id, extract_date)
        elif api_name == "conversation_details_job":
            logger.info("Conversation details job kick off")
            exec_conversation_details_job(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "evaluation_forms":
            exec_evaluation_forms_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "evaluations":
            exec_evaluations_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "users_details_job":
            exec_users_details_job_api(spark, tenant, run_id, db_name, extract_date)
        elif api_name == "wfm_adherence":
            exec_wfm_adherence_api(spark, tenant, run_id, db_name, extract_date)
        else:
            logger.exception("invalid api name")

    except Exception as e:
        logger.exception(traceback.print_exc())
