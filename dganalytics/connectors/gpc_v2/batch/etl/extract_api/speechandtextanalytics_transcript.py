import json
import requests as requests
import time
from pyspark.sql import SparkSession

from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger

def get_conversations(spark: SparkSession, extract_start_time: str, extract_end_time: str) :
    conversations_df = spark.sql(f"""
       SELECT   DISTINCT  C.conversationId,
                          C.sessionId communicationId
        FROM gpc_simplyenergy.raw_speechandtextanalytics RS
        JOIN (SELECT  conversationId,
                            sessions.sessionId sessionId
                    FROM (SELECT  conversationId,
                                    EXPLODE(participants.sessions) sessions
                            FROM (SELECT  conversationId,
                                        explode(participants) as participants
                                FROM gpc_simplyenergy.raw_conversation_details
                )
        WHERE participants.purpose = 'customer')) C
        ON RS.conversation.id = c.conversationId
        WHERE extractIntervalStartTime = '{extract_start_time}'
              AND extractIntervalEndTime = '{extract_end_time}'
    """)

    return conversations_df

def exec_speechandtextanalytics_transcript(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_speechandtextanalytics_transcript")
    base_url = get_api_url(tenant)
    auth_headers = authorize(tenant)

    logger.info(f"Getting conversationId and communicationId extracted for {extract_start_time} to {extract_end_time} for Speech And Text Analytics Transript")

    conversations = get_conversations(spark, extract_start_time, extract_end_time)
    convs_df = conversations.toPandas()
    transcripts = []
    for conversation in convs_df.itertuples():
        conversationId = conversation.conversationId
        communicationId = conversation.communicationId

        url = f"{base_url}/api/v2/speechandtextanalytics/conversations/{conversationId}/communications/{communicationId}/transcripturl"
        resp = requests.request(method="GET", url=url, headers=auth_headers)
        if resp.status_code == 404:
            continue
        if resp.status_code != 200:
            logger.exception(
                "Speech and Text Analytic's transcript Url failed: " + str(resp.text))
            continue
        
        url = resp.json()['url']

        transcript_resp = requests.get(url)

        if transcript_resp.status_code == 404:
            continue
        if transcript_resp.status_code != 200:
             logger.exception(
                "Speech and Text Analytic's transcript Analytics failed: " + str(resp.text))
             continue
        
        transcripts.append(transcript_resp.json())

    transcripts = [json.dumps(b) for b in transcripts]

    if len(transcripts) > 0:
        process_raw_data(spark, tenant, 'speechandtextanalytics_transcript', run_id, transcripts, extract_start_time, extract_end_time, len(transcripts))