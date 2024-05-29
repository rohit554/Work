import json
import requests as requests
import time
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc_v2.gpc_utils import authorize, get_api_url, process_raw_data
from dganalytics.connectors.gpc_v2.gpc_utils import gpc_utils_logger
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.functions import col

def get_conversations(spark: SparkSession, extract_start_time: str, extract_end_time: str) :
    conversations_df = spark.sql(f"""
       

SELECT DISTINCT conversationId, sessions.sessionId communicationId
FROM(  SELECT
          conversationId,
          EXPLODE(participants.sessions) sessions
        FROM
          (
            SELECT
              conversationId,
              explode(participants) as participants
            FROM
              gpc_hellofresh.raw_conversation_details
            WHERE --extractDate = '2024-04-29'
              conversationStart BETWEEN '{extract_start_time}'
              and '{extract_end_time}'
          )
        WHERE
          participants.purpose = 'customer') c

where exists (SELECT 1
--count( distinct conversationId)--,
  -- case when purpose = 'customer' then sessionId end sessionId
FROM
  (
    SELECT
      conversationId,
      sessions.sessionId sessionId,
      explode(sessions.segments) segment,
      purpose
    FROM
      (
        SELECT
          conversationId,
          EXPLODE(participants.sessions) sessions,
          participants.purpose
        FROM
          (
            SELECT
              conversationId,
              explode(participants) as participants
            FROM
              gpc_hellofresh.raw_conversation_details
            WHERE --extractDate = '2024-04-29'
              conversationStart BETWEEN '{extract_start_time}'
              and '{extract_end_time}'
          )
        -- WHERE
        --   participants.purpose = 'customer'
      )
    where
      sessions.mediaType = 'voice'
  ) d
where
  d.segment.wrapUpCode in (
    select
      wrapupId
    from
      gpc_hellofresh.dim_wrapup_codes wc
    where
      wc.wrapupCode in (
        'Z-Out-Active',
        'Z-OUT-Failure',
        'Z-Out-ManualCallback',
        'Z-OUT-Failure-Hang-up',
        'Z-OUT-Unable to reach',
        'Z-Out-Failed Payment',
        'Z-Out-NoPermission',
        'Z-Out Not Eligible',
        'Z-OUT-Bad Number',
        'Z-OUT-Success-1Week',
        'Z-OUT-Success-2Week',
        'Z-OUT-Success-3Week',
        'Z-OUT-Success-4Week',
        'Z-OUT-Success-5+Week-TL APPROVED'
      )
  )
  and d.segment.queueId in (
    select
      queueId
    from
      gpc_hellofresh.dim_routing_queues
    where
      queueName in (
        'AU Sales',
        'AU Sales 12W',
        'AU Sales 18W',
        'AU Sales 30W',
        'AU Sales 30W WA',
        'AU Sales 18W WA',
        'AU Sales OFF BAU',
        'AU HF WA Sales',
        'NZ Sales',
        'NZ Sales 12W',
        'NZ Sales 30W',
        'NZ Sales W18',
        'AU EveryPlate Sales',
        'AU EveryPlate W12 Sales',
        'AU EveryPlate W12 WA Sales',
        'AU EveryPlate W18 Sales',
        'AU EveryPlate W18 WA Sales',
        'AU EveryPlate W30 Sales',
        'AU EveryPlate W30 WA Sales',
        'AU YouFoodz Sales W30'
      )
  )
  and d.conversationId = c.conversationId
  )
        
    """)

    return conversations_df

def exec_speechandtextanalytics_transcript(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_speechandtextanalytics_transcript")
    base_url = get_api_url(tenant)
    auth_headers = authorize(tenant)

    logger.info(f"Getting conversationId and communicationId extracted for {extract_start_time} to {extract_end_time} for Speech And Text Analytics Transcript")

    conversations = get_conversations(spark, extract_start_time, extract_end_time)

    transcripts = []

    with ThreadPoolExecutor(max_workers=100) as executor:
        for conversation in conversations.select("conversationId", "communicationId").rdd.collect():
            conversation_id = conversation.conversationId
            communication_id = conversation.communicationId

            url = f"{base_url}/api/v2/speechandtextanalytics/conversations/{conversation_id}/communications/{communication_id}/transcripturl"
            resp = requests.get(url, headers=auth_headers)

            if resp.status_code == 404:
                continue
            if resp.status_code != 200:
                logger.exception("Speech and Text Analytic's transcript URL failed: " + str(resp.text))
                continue

            url = resp.json().get('url')

            if url is None:
                continue

            transcript_resp = requests.get(url)

            if transcript_resp.status_code == 404:
                continue
            if transcript_resp.status_code != 200:
                logger.exception("Speech and Text Analytic's transcript Analytics failed: " + str(resp.text))
                continue

            transcripts.append(transcript_resp.json())

    transcripts = [json.dumps(b) for b in transcripts if b]

    if len(transcripts) > 0:
        process_raw_data(spark, tenant, 'speechandtextanalytics_transcript', run_id, transcripts, extract_start_time, extract_end_time, len(transcripts))