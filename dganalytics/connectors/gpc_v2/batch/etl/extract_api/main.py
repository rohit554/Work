from dganalytics.utils.utils import get_spark_session, flush_utils
from dganalytics.connectors.gpc_v2.gpc_utils import get_dbname, gpc_request, extract_parser, gpc_utils_logger
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.conversation_details_job import exec_conversation_details_job
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.evaluation_forms import exec_evaluation_forms_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.evaluations import exec_evaluations_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.surveys import exec_surveys_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.users_details_job import exec_users_details_job_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.wfm_adherence import exec_wfm_adherence_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.business_units import exec_business_units
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.management_units import exec_management_units
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.management_unit_users import exec_management_unit_users
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.activity_codes import exec_activity_codes
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.wfm_forecast import exec_wfm_forecast_api
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.wfm_planninggroups import exec_wfm_planninggroups
from dganalytics.connectors.gpc_v2.batch.etl.extract_api.speechminer import exec_speechminer

if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "gpc_extract_" + api_name
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)
    logger = gpc_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting GPC API {api_name}")
        if api_name in ["users", "routing_queues", "groups", "users_details", "wrapup_codes",
                        "conversation_details", "divisions", "presence_definitions"]:
            df = gpc_request(spark, tenant, api_name, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "conversation_details_job":
            logger.info("Conversation details job kick off")
            exec_conversation_details_job(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "evaluation_forms":
            logger.info("Evaluation Forms kick off")
            exec_evaluation_forms_api(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "evaluations":
            exec_evaluations_api(spark, tenant, run_id,
                                 extract_start_time, extract_end_time)
        elif api_name == "surveys":
            exec_surveys_api(spark, tenant, run_id,
                             extract_start_time, extract_end_time)
        elif api_name == "users_details_job":
            exec_users_details_job_api(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "wfm_adherence":
            exec_wfm_adherence_api(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "business_units":
            exec_business_units(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "management_units":
            exec_management_units(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "management_unit_users":
            exec_management_unit_users(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "activity_codes":
            exec_activity_codes(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "wfm_forecast":
            exec_wfm_forecast_api(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "wfm_planninggroups":
            exec_wfm_planninggroups(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        elif api_name == "speechminer":
            exec_wfm_planninggroups(
                spark, tenant, run_id, extract_start_time, extract_end_time)
        else:
            logger.exception("invalid api name")
            raise Exception

    except Exception as e:
        logger.exception(
            f"Error Occured in GPC Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}")
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)
