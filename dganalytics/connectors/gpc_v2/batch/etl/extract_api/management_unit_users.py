from pyspark.sql import SparkSession
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger, gpc_request
import json

def get_muids(spark: SparkSession) -> list:
    muids = spark.sql("""select DISTINCT  id as muid from raw_management_units """).toPandas()[
        'muid'].tolist()
    return muids


def exec_management_unit_users(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = gpc_utils_logger(tenant, "gpc_evaluations")

    muids = get_muids(spark)
    management_unit_users = []
    for b in muids:
        api_config = {
            "management_unit_users": {
                "endpoint": f"/api/v2/workforcemanagement/managementunits/{b}/users"
            }
        }
        '''
        resp = rq.get(f"{get_api_url(tenant)}/api/v2/workforcemanagement/businessunits/{b}/managementunits",
                      headers=api_headers)
        if resp.status_code != 200:
            logger.exception("Detailed Evaluations API Failed" + resp.text)
        management_units.append(resp.json()['entities'])
        '''
        lst = gpc_request(spark, tenant, 'management_unit_users', run_id,
                          extract_start_time, extract_end_time,
                          api_config, skip_raw_load=True)
        lst = [json.loads(val) for val in lst]
        [val.__setitem__('managementId', b) for val in lst]
        lst = [json.dumps(val) for val in lst]
        management_unit_users.append(lst)

    management_unit_users = [
        item for sublist in management_unit_users for item in sublist]
    # management_units = [json.dumps(ed)
    #                    for ed in management_units]
    process_raw_data(spark, tenant, 'management_unit_users', run_id,
                     management_unit_users, extract_start_time, extract_end_time, len(management_unit_users))
