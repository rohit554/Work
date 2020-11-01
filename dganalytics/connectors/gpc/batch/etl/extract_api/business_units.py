import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import gpc_request, authorize, check_api_response
from dganalytics.connectors.gpc.gpc_utils import get_api_url, gpc_utils_logger, process_raw_data


def exec_business_units(spark: SparkSession, tenant: str, run_id: str,
                        extract_start_time: str, extract_end_time: str):
    logger = gpc_utils_logger(tenant, 'exec_business_units')
    api_headers = authorize(tenant)
    bus = rq.get(
        f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits", headers=api_headers)
    business_units = []
    if check_api_response(bus, "divisions", tenant, run_id) == 'OK':
        bu_ids = [id['selfUri'] for id in bus.json()['entities']]
        buid = bu_ids[0]
        for buid in bu_ids:
            bu = rq.get(
                f"{get_api_url(tenant)}{buid}?expand=settings", headers=api_headers)
            if check_api_response(bu, "business unit", tenant, run_id) == 'OK':
                business_units.append(bu.json())

        business_units = [json.dumps(b) for b in business_units]
        process_raw_data(spark, tenant, 'business_units', run_id,
                         business_units, extract_start_time, extract_end_time, len(business_units))
