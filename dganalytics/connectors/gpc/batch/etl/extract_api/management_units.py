import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, gpc_request


def get_buids(spark: SparkSession) -> list:
    buids = spark.sql("""select DISTINCT  id as buid from raw_business_units """).toPandas()[
        'buid'].tolist()
    return buids


def exec_management_units(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = gpc_utils_logger(tenant, "gpc_evaluations")

    buids = get_buids(spark)
    api_headers = authorize(tenant)
    management_units = []
    for b in buids:
        api_config = {
            "management_units": {
                "endpoint": f"/api/v2/workforcemanagement/businessunits/{b}/managementunits"
            }
        }
        '''
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{b}/managementunits",
                      headers=api_headers)
        if resp.status_code != 200:
            logger.exception("Detailed Evaluations API Failed" + resp.text)
        management_units.append(resp.json()['entities'])
        '''
        management_units.append(gpc_request(spark, tenant, 'management_units', run_id,
                                            extract_start_time, extract_end_time,
                                            api_config, skip_raw_load=True))

    management_units = [
        item for sublist in management_units for item in sublist]
    # management_units = [json.dumps(ed)
    #                    for ed in management_units]
    process_raw_data(spark, tenant, 'management_units', run_id,
                     management_units, extract_start_time, extract_end_time, len(management_units))
