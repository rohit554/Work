import json
import requests as requests
import time
from pyspark.sql import SparkSession

from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger

def get_conversations(spark: SparkSession, extract_start_time: str, extract_end_time: str) -> list:
    conversations_df = spark.sql(f"""
        SELECT
            DISTINCT conversationId
        FROM
            raw_conversation_details
        WHERE
            extractIntervalStartTime = '{extract_start_time}'
            AND extractIntervalEndTime = '{extract_end_time}'
            AND conversationId IS NOT NULL
    """)

    return conversations_df


def get_sentiments(base_url: str, auth_headers, conversation_id: str, retry_count: int):
    url = f"{base_url}/api/v2/speechandtextanalytics/conversations/{conversation_id}"
    resp = requests.request(method="GET", url=url, headers=auth_headers)

    if resp.status_code == 429:
        retry_count += 1

        if retry_count > 3:
            logging.error(resp)
            raise Exception

        logger.info(f"Rate limit exceeded for get sentiments API call, sleeping for 30 seconds, retry count {retry_count} of 3")
        time.sleep(30)

        return get_sentiments(base_url, auth_headers, conversation_id, retry_count)
    elif resp.status_code == 404:
        logger.info(f"No sentiments found for conversation {conversation_id}")
        return {}
    elif resp.status_code != 200:
        logger.error(resp)
        raise Exception

    return resp.json()


def exec_speechminer(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_sentiments")
    logger.info(f"getting conversations extracted between {extract_start_time} and {extract_end_time} for sentiments")
    conversations_df = get_conversations(spark, extract_start_time, extract_end_time)
    auth_headers = authorize(tenant)
    base_url = get_api_url(tenant)
    sentiments = []

    for conversation in conversations_df.rdd.collect():
        conversation_id = conversation['conversationId']
        resp_json = get_sentiments(base_url, auth_headers, conversation_id, 0)

        if resp_json != None and len(resp_json) > 0:
            logger.info(f"adding {len(resp_json)} sentiment(s) for conversation {conversation_id}")
            sentiments.append(resp_json)

    if len(sentiments) > 0:
        process_raw_data(spark, tenant, 'sentiments', run_id, sentiments, extract_start_time, extract_end_time, len(sentiments))
