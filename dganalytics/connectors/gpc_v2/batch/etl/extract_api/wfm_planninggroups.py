import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_secret
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import get_interval, gpc_utils_logger
import ast
from websocket import create_connection
import time
from datetime import datetime, timedelta


def get_business_units_list(spark: SparkSession):
    business_units_pdf = spark.sql("select id as raw_business_unit_id from raw_business_units").toPandas()
    # ['userId'].tolist()
    business_units_list = business_units_pdf.raw_business_unit_id.to_list()
    return business_units_list


def get_wfm_planninggroups(tenant: str, businessUnitId: str):
    logger = gpc_utils_logger(tenant, "wfm_forecast_id_list")

    api_headers = authorize(tenant)

    planninggroups_data = rq.get(
        f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{businessUnitId}/planninggroups", headers=api_headers)
    if planninggroups_data.status_code != 200:
        logger.exception("planninggroups_data API Failed %s",
                        forecast_list_data.text)

    planninggroups_data = planninggroups_data.json()
    # steaming_channel_id = steaming_channel['id']
    planninggroups_data = planninggroups_data['entities']
    for _ in planninggroups_data:
        _['businessUnitId']=businessUnitId
    planninggroups_data = [json.dumps(_) for _ in planninggroups_data]
    return(planninggroups_data)

def exec_wfm_planninggroups(
    spark: SparkSession,
    tenant: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str
):
    logger = gpc_utils_logger(tenant, "wfm_planninggroups")
    # api_headers = authorize(tenant)
    business_units_list = get_business_units_list(spark)
    final_planninggroups_list = []
    for businessUnitId in business_units_list:
        planninggroups_list = get_wfm_planninggroups(tenant, businessUnitId)
        for _ in planninggroups_list:
            final_planninggroups_list.append(_)
    process_raw_data(spark, tenant, 'wfm_planninggroups', run_id,
            final_planninggroups_list, extract_start_time, extract_end_time, len(final_planninggroups_list))
