import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import get_api_url, gpc_request
from dganalytics.connectors.gpc.gpc_utils import authorize, process_raw_data
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger
from datetime import datetime, timedelta
import math


def exec_conversation_aggregates(spark: SparkSession, tenant: str, run_id: str,
                                 extract_start_time: str, extract_end_time: str):
    logger = gpc_utils_logger(tenant, "exec_conversation_aggregates")
    logger.info("Extracting exec_conversation_aggregates")
    conv_agg = []

    start = datetime.strptime(extract_start_time, '%Y-%m-%dT%H:%M:%S')
    end = datetime.strptime(extract_end_time, '%Y-%m-%dT%H:%M:%S')

    for i in range(0, math.ceil((end - start).seconds / 3600)):
        print(
            f"conversation aggregates extracting for interval - {start + timedelta(hours=i)} to {start + timedelta(hours=i + 1)}")
        resp_list = gpc_request(spark, tenant, 'conversation_aggregates', run_id,
                                (start + timedelta(hours=i)
                                 ).strftime('%Y-%m-%dT%H:%M:%S'),
                                (start + timedelta(hours=i + 1)
                                 ).strftime('%Y-%m-%dT%H:%M:%S'),
                                skip_raw_load=True)
        conv_agg = conv_agg + resp_list

    # conv_agg = [json.dumps(conv) for conv in conv_agg]

    process_raw_data(spark, tenant, 'conversation_aggregates', run_id,
                     conv_agg, extract_start_time, extract_end_time, len(conv_agg))
