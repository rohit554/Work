import requests as rq
import json
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import gpc_request, authorize, check_api_response
from dganalytics.connectors.gpc.gpc_utils import get_api_url, gpc_utils_logger, process_raw_data


def exec_speechandtextanalytics_topics(spark: SparkSession, tenant: str, run_id: str):
    logger = gpc_utils_logger(tenant, 'exec_speechandtextanalytics_topics')
    api_headers = authorize(tenant)
    topics = rq.get(
        f"{get_api_url(tenant)}/api/v2/speechandtextanalytics/topics", headers=api_headers)
    speechandtextanalytics_topics = []
    
    for topic in topics.json()['entities']:
        speechandtextanalytics_topics.append(topic)

    speechandtextanalytics_topics = [json.dumps(b) for b in speechandtextanalytics_topics]
    process_raw_data(spark, tenant, 'speechandtextanalytics_topics', run_id,
                        speechandtextanalytics_topics, extract_start_time, extract_end_time, len(speechandtextanalytics_topics))