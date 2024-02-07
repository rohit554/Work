import requests
import json
from pyspark.sql import SparkSession
import datetime
from dganalytics.helios.helios_utils import helios_utils_logger
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, DateType
import concurrent.futures
import threading
from dganalytics.utils.utils import get_secret, get_path_vars
import pandas as pd
from datetime import datetime
import os
import sys

#TODO: remove this function later and read schema using from dganalytics.connectors.gpc_v2.gpc_utils import get_schema
def get_api_schema():
    return StructType([
        StructField("resolved_reason", StringType(), True),
        StructField("additional_service", StringType(), True),
        StructField("process_knowledge", StringType(), True),
        StructField("conversation_id", StringType(), True),
        StructField("system_knowledge", StringType(), True),
        StructField("satisfaction", StringType(), True),
        StructField("resolved", StringType(), True),
        StructField("contact", ArrayType(
            StructType([
                StructField("contact_reason", StringType(), True),
                StructField("inquiries", ArrayType(
                    StructType([
                        StructField("inquiry_type", StringType(), True),
                        StructField("main_inquiry", StringType(), True),
                        StructField("root_cause", StringType(), True)
                    ])
                ), True)
            ])
        ), True),
        StructField("process_map", ArrayType(
            StructType([
                StructField("call_status", StringType(), True),
                StructField("emotion", StringType(), True),
                StructField("line", StringType(), True),
                StructField("action", StringType(), True),
                StructField("category", StringType(), True),
                StructField("action_label", StringType(), True),
                StructField("status", StringType(), True)
            ])
        ), True),
        StructField("extractDate", DateType(), True),
        StructField("extractIntervalStartTime", TimestampType(), True),
        StructField("extractIntervalEndTime", TimestampType(), True),
        StructField("recordInsertTime", TimestampType(), True),
        StructField("recordIdentifier", IntegerType(), True)
  ])

# Get Access token for calling Lambda Function
def getAccessToken():
    # Lambda function authentication URL
    url = get_secret("awsLambdaAuthURL")

    payload = json.dumps({
        "user_id": get_secret("awsLambdaAuthUserId"),
        "user_password": get_secret("awsLambdaAuthPassword")
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    if response.status_code != 200:
        raise "Error"
    return response.json()['data']

def get_conversation_transcript(spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str):
    return spark.sql(f"""
        SELECT conversationId, concat_ws("\\n", collect_list(phrase)) conversation
        FROM (SELECT P.conversationId, concat(string(row_number() OVER(PARTITION BY P.conversationId ORDER BY startTimeMs)), ':', participantPurpose, ':', text) phrase FROM gpc_{tenant}.fact_conversation_transcript_phrases P
        INNER JOIN gpc_{tenant}.raw_speechandtextanalytics_transcript C
        ON P.conversationId = c.conversationId
        where c.extractDate = CAST('{extract_start_time}' AS DATE) AND c.extractIntervalStartTime = '{extract_start_time}' and c.extractIntervalEndTime = '{extract_end_time}'
        )
        GROUP BY conversationId
    """)

def process_conversation(spark, conv, url, tenant, extract_start_time, extract_end_time):
    logger = helios_utils_logger(tenant, "transcript_insights")
    if sys.getsizeof(conv.conversation) < 1024 :  
      logger.exception(F"Small conversation: {conv.conversationId}")
      return {'status': False, "conversationId": conv.conversationId, "Small Conversation": 500 }
    else:
        schema = get_api_schema()
        token = getAccessToken()

        headers = {
            'dg-token': token,
            'Content-Type': 'application/json'
        }

        payload = json.dumps({
            "conversation_id": conv.conversationId,
            "conversation": conv.conversation,
            "tenant_id": tenant
        })

        retry = 0
        while retry < 3:    
            # Get Lambda Function Response
            response = requests.request("POST", url, headers=headers, data=payload)
            retry = retry + 1

            if response.status_code != 502:
                break

        if response.status_code == 200:
            data = response.json()['data']

            resp = [data]
            
            try:
                insights = spark.createDataFrame(resp, schema=schema)
                insights.createOrReplaceTempView('insights')
                spark.sql(f"""
                        INSERT INTO gpc_{tenant}.raw_transcript_insights (additional_service, process_knowledge, conversation_id, contact, system_knowledge, process_map, satisfaction, resolved, extractDate, extractIntervalStartTime, extractIntervalEndTime, recordInsertTime, recordIdentifier)
                        SELECT additional_service, process_knowledge, conversation_id, contact, system_knowledge, process_map, satisfaction, resolved,CAST('{extract_start_time}' AS DATE) extractDate, '{extract_start_time}' extractIntervalStartTime, '{extract_end_time}' extractIntervalEndTime, '{datetime.now()}' recordInsertTime,
                        1 recordIdentifier  FROM insights
                        """)
                return {'status': True, "conversationId":conv.conversationId, "error_or_status_code": response.status_code }
            except Exception as e:
                logger.exception(f"Error Occurred in insights insertion for conversation: {conv.conversationId}")
                logger.exception(e, stack_info=True, exc_info=True)
                return {'status': False, "conversationId":conv.conversationId, "error_or_status_code": e }
            
        if response.status_code != 200:
            logger.exception(F"Error occurred with conversation: {conv.conversationId}, STATUS_CODE: {response.status_code}")
            return {'status': False, "conversationId": conv.conversationId, "error_or_status_code": response.status_code }
        

def pool_executor(spark, conversations, url, tenant, extract_start_time, extract_end_time, results):
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_conversation, spark, conv, url, tenant, extract_start_time, extract_end_time) for conv in conversations.toPandas().itertuples()]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

def get_transcript_insights(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    logger = helios_utils_logger(tenant, "transcript_insights")
    conversations = get_conversation_transcript(spark, tenant, extract_start_time, extract_end_time)

    # Splitting the conversations into two halves for two APIs
    total_conversations = conversations.count()
    print("Total Conversations to process: ", total_conversations)
    copy_df = conversations

    half = total_conversations // 2

    # Get the top `each_len` number of rows 
    temp_df = copy_df.limit(half) 
    first_half_conversations = temp_df
  
    # Truncate the `copy_df` to remove 
    # the contents fetched for `temp_df` 
    copy_df = copy_df.subtract(temp_df) 
    second_half_conversations = copy_df

    url1 = get_secret("awsLambdaUSEastURL")
    url2 = get_secret("awsLambdaUSWestURL")
    results1 = []
    results2 = []
    # Creating threads to process each half in parallel
    thread1 = threading.Thread(target=pool_executor, args=(spark, first_half_conversations, url1, tenant, extract_start_time, extract_end_time, results1))
    thread2 = threading.Thread(target=pool_executor, args=(spark, second_half_conversations, url2, tenant, extract_start_time, extract_end_time, results2))

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()

    result = results1 + results2
    print("Total processed conversations: ", len(result))
    df = pd.DataFrame(result)
    tenant_path, db_path, log_path = get_path_vars(tenant)
    df.to_csv(os.path.join(tenant_path, 'data', 'raw', 'audit', f'audit_{datetime.now()}.csv'),
                                       header=True, index=False)