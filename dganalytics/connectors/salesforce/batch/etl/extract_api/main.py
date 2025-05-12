from dganalytics.utils.utils import get_spark_session, flush_utils, get_path_vars
from dganalytics.connectors.salesforce.sf_utils import (
    get_dbname,
    sf_request,
    extract_parser,
    sf_utils_logger,
)
from dganalytics.connectors.salesforce.batch.etl.extract_api.account import exec_account
from dganalytics.connectors.salesforce.batch.etl.extract_api.case import exec_case
from dganalytics.connectors.salesforce.batch.etl.extract_api.contact import exec_contact
from dganalytics.connectors.salesforce.batch.etl.extract_api.event import exec_event
from dganalytics.connectors.salesforce.batch.etl.extract_api.lead import exec_lead
from dganalytics.connectors.salesforce.batch.etl.extract_api.order import exec_order
from dganalytics.connectors.salesforce.batch.etl.extract_api.task import exec_task
from dganalytics.connectors.salesforce.batch.etl.extract_api.opportunity import exec_opportunity
from dganalytics.connectors.salesforce.batch.etl.extract_api.eventlogfile import exec_eventlogfile

# TODO: Move these to respective files 
from dganalytics.connectors.salesforce.sf_api_config import sf_end_points
from dganalytics.connectors.salesforce.sf_utils import (
    process_raw_data,
    sf_utils_logger,
    get_schema,
    get_spark_partitions_num,
    get_dbname,
    extract_parser,
)
import json
from pyspark.sql.functions import to_json, collect_list, from_json, explode
import os, shutil


def get_entity_name(api_name):
    current_api = sf_end_points[api_name]
    if "entity_name" in current_api:
        return current_api["entity_name"]
    return ""


if __name__ == "__main__":
    tenant, run_id, extract_start_time, extract_end_time, api_name = extract_parser()
    db_name = get_dbname(tenant)
    app_name = "sf_extract_" + api_name
    spark = get_spark_session(app_name=app_name, tenant=tenant, default_db=db_name)
    logger = sf_utils_logger(tenant, app_name)

    try:
        logger.info(f"Extracting Salesforce API {api_name}")
        if api_name == "account":
            logger.info("account details job kick off")
            exec_account(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "case":
            logger.info("case details job kick off")
            exec_case(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "contact":
            logger.info("contact details job kick off")
            exec_contact(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "event":
            logger.info("event details job kick off")
            exec_event(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "lead":
            logger.info("lead details job kick off")
            exec_lead(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "order":
            logger.info("order details job kick off")
            exec_order(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "opportunity":
            logger.info("opportunity details job kick off")
            exec_opportunity(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "task":
            logger.info("task details job kick off")
            exec_task(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        elif api_name == "eventlogfile":
            logger.info("eventlogfile details job kick off")
            exec_eventlogfile(
                spark, tenant, api_name, run_id, extract_start_time, extract_end_time
            )
        else:
            logger.exception("invalid api name")
            raise Exception

    except Exception as e:
        logger.exception(
            f"Error Occured in Salesforce Extraction for {extract_start_time}_{extract_end_time}_{tenant}_{api_name}"
        )
        logger.exception(e, stack_info=True, exc_info=True)
        raise Exception
    finally:
        flush_utils(spark, logger)