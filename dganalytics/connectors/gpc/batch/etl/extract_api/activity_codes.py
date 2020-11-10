import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger, gpc_request


def get_buids(spark: SparkSession) -> list:
    buids = spark.sql("""select DISTINCT  id as buid from raw_business_units """).toPandas()[
        'buid'].tolist()
    return buids


def exec_activity_codes(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):

    logger = gpc_utils_logger(tenant, "gpc_activity_codes")

    buids = get_buids(spark)
    activity_codes = []
    for b in buids:
        api_config = {
            "activity_codes": {
                "endpoint": f"/api/v2/workforcemanagement/businessunits/{b}/activitycodes"
            }
        }
        resp = gpc_request(spark, tenant, 'activity_codes', run_id,
                                          extract_start_time, extract_end_time,
                                          api_config, skip_raw_load=True)
        for r in resp:
            j = json.loads(r)
            j.update({'businessUnitId': b})
            activity_codes.append(json.dumps(j))

    process_raw_data(spark, tenant, 'activity_codes', run_id,
                     activity_codes, extract_start_time, extract_end_time, len(activity_codes))
