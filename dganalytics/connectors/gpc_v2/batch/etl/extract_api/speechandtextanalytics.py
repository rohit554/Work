import json
import requests
import time
from pyspark.sql import SparkSession
from socket import error as SocketError
import errno
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor

def get_conversations(spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str) -> list:
    conversations_df = spark.sql(f"""
        SELECT  DISTINCT conversationId
        FROM (SELECT  conversationId,
                        EXPLODE(participants.sessions) sessions
                FROM (SELECT  conversationId,
                            explode(participants) as participants
                    FROM gpc_{tenant}.raw_conversation_details
                    WHERE   extractIntervalStartTime = '{extract_start_time}'
                            AND extractIntervalEndTime = '{extract_end_time}'
                            AND conversationId IS NOT NULL
                            AND NOT EXISTS (select 1 FROM gpc_{tenant}.fact_speechandtextanalytics fs where fs.conversationId = raw_conversation_details.conversationId)
                )
            WHERE participants.purpose IN( 'customer', 'external'))
       WHERE sessions.mediaType IN ('voice', 'chat', 'email')
    """)

    return conversations_df


def get_speechandtextanalytics(base_url: str, auth_headers, conversation_id: str, retry_count: int):
    url = f"{base_url}/api/v2/speechandtextanalytics/conversations/{conversation_id}"
    try:
        resp = requests.request(method="GET", url=url, headers=auth_headers)
    except SocketError as e:
        if e.errno != errno.ECONNRESET:
            raise Exception
    except Exception as err:
        pass

    if resp.status_code == 429:
        retry_count += 1

        if retry_count > 3:
            logger.error(resp)
            raise Exception

        logger.info(f"Rate limit exceeded for get Speech And Text Analytics API call, sleeping for 30 seconds, retry count {retry_count} of 3")
        time.sleep(30)

        # return get_speechandtextanalytics(base_url, auth_headers, conversation_id, retry_count)
        return {}
    elif resp.status_code == 404:
        logger.info(f"No Speech And Text Analytics found for conversation {conversation_id}")
        return {}
    elif resp.status_code != 200:
        logger.error(resp)
        raise Exception

    return resp.json()


def exec_speechandtextanalytics(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_speechandtextanalytics")
    logger.info(f"getting conversations extracted between {extract_start_time} and {extract_end_time} for Speech And Text Analytics")

    conversations_df = get_conversations(spark, tenant, extract_start_time, extract_end_time)

    if conversations_df is None or not hasattr(conversations_df, 'rdd'):
        logger.error("Failed to retrieve valid conversations DataFrame. Exiting thread execution.")
        return

    auth_headers = authorize(tenant)
    base_url = get_api_url(tenant)
    speechandtextanalytics = []

    valid_conversations = conversations_df.filter(conversations_df['conversationId'].isNotNull())

    with ThreadPoolExecutor(max_workers = 20) as executor:
        results = executor.map(lambda conversation: get_speechandtextanalytics(base_url, auth_headers, conversation['conversationId'], 0), valid_conversations.rdd.collect())

    for resp_json in results:
        if resp_json is not None and len(resp_json) > 0:
            logger.info(f"adding {len(resp_json)} Speech And Text Analytics for a conversation")
            speechandtextanalytics.append(resp_json)

    if len(speechandtextanalytics) > 0:
        process_raw_data(spark, tenant, 'speechandtextanalytics', run_id, speechandtextanalytics, extract_start_time, extract_end_time, len(speechandtextanalytics))