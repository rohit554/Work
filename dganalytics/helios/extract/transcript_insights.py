import requests
import json
from pyspark.sql import SparkSession
import datetime
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, DateType
import concurrent.futures
import threading
from dganalytics.utils.utils import get_secret, get_path_vars
import pandas as pd
from datetime import datetime
import os
import sys
from dganalytics.helios.helios_utils import helios_utils_logger


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
                        StructField("root_cause", StringType(), True),
                        StructField("additional_inquiry", StringType(), True),
                        StructField("main_inquiry_raw", StringType(), True),
                        StructField("root_cause_raw", StringType(), True)
                    ])
                ), True),
                StructField("contact_reason_raw", StringType(), True)

            ])
        ), True),
        StructField("process_map", ArrayType(
            StructType([
                StructField("call_status", StringType(), True),
                StructField("emotion", StringType(), True),
                StructField("speaker", StringType(), True),
                StructField("start_line", StringType(), True),
                StructField("end_line", StringType(), True),
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

def get_conversation_transcript(spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str, conversationId : str):
    return spark.sql(f"""
        SELECT conversationId, concat_ws("\\n", collect_list(phrase)) conversation, conversationStartDateId
        FROM (
          SELECT P.conversationId, concat(string(row_number() OVER(PARTITION BY P.conversationId ORDER BY P.startTimeMs)), ':', P.participantPurpose, ':', text) phrase, C.conversationStartDateId FROM gpc_{tenant}.fact_conversation_transcript_phrases P
          INNER JOIN dgdm_{tenant}.dim_conversations C
          ON P.conversationId = C.conversationId
          where P.conversationId = '{conversationId}'
         )
        GROUP BY conversationId, conversationStartDateId        
    """).collect()[0]

def insert_insights_audit(spark: SparkSession, tenant: str, status : bool, conversationId : str , error_or_status_code : str, transcriptSize : int,transcriptProcessingStartTime : datetime,      transcriptProcessingEndTime : datetime,conversationStartDateId : int,error:str , url:str):
  spark.sql(f"""
          INSERT INTO dgdm_{tenant}.transcript_insights_audit 
          VALUES (
            {status},
            '{conversationId}',
            '{error_or_status_code}',
            {transcriptSize},
            '{transcriptProcessingStartTime}',
            '{transcriptProcessingEndTime}',
            {conversationStartDateId},
            '{error}',
            '{url}',
            date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')
        )
  """)

def get_conversations(spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str):
  return spark.sql(f"""
    SELECT distinct P.conversationId FROM gpc_simplyenergy.fact_conversation_transcript_phrases P
    INNER JOIN dgdm_simplyenergy.dim_conversations C
    ON P.conversationId = C.conversationId
    where C.conversationStartDateId = date_format('{extract_start_time}', 'yyyyMMdd') 
            and not exists  (select 1  FROM dgdm_simplyenergy.fact_transcript_insights i
                                    where i.conversationStartDateId = date_format('{extract_start_time}', 'yyyyMMdd')
                                    and i.conversationId = p.conversationId
                                )
  """)


def process_conversation(spark, conv, url, tenant, extract_start_time, extract_end_time, schema, token):
    startTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    print(conv['conversationId'])
    conv = get_conversation_transcript(spark, tenant, extract_start_time, extract_end_time, conv['conversationId'])
    size = sys.getsizeof(conv['conversation'])
    # print(conv)
    logger = helios_utils_logger(tenant, "transcript_insights")
    urldisplay=''
    if url == get_secret("awsLambdaUSEastURL"):
        urldisplay = 'USEastURL'
    else:
      urldisplay = 'USWestURL'
    # print(urldisplay)
    if size < 1024 :  
      logger.exception(F"Small conversation: {conv['conversationId']}")
      endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
      insert_insights_audit(spark, tenant, False, conv['conversationId'] , 400, size,startTime, endTime,conv['conversationStartDateId'],'Small Conversation',urldisplay)

    else:
        headers = {
            'dg-token': token,
            'Content-Type': 'application/json'
        }

        payload = json.dumps({
            "conversation_id": conv['conversationId'],
            "conversation": conv['conversation'],
            "tenant_id": tenant
        })

        
        retry = 0
        while retry < 3:    
            response = None
            # Get Lambda Function Response
            try:
                response = requests.request("POST", url, headers=headers, data=payload, timeout = 90)
                if response.status_code == 200:
                    break
            except requests.Timeout as T :
                endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.exception(f"Timeout for conversation: {conv['conversationId']}")
                logger.exception(T, stack_info=True, exc_info=True)
                insert_insights_audit(spark, tenant, False,conv['conversationId'],408, size,startTime,endTime,conv['conversationStartDateId'],'Conversation Timed Out',urldisplay)
            except  Exception as e:
                endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.exception(e, stack_info=True, exc_info=True)
                insert_insights_audit(spark, tenant, False,conv['conversationId'],500, size,startTime,endTime,conv['conversationStartDateId'],e,urldisplay)
            
            retry = retry + 1


        if response.status_code == 200:
            data = response.json()['data']
            endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
            resp = [data]
            
            try:
                insights = spark.createDataFrame(resp, schema=schema)
                insights.createOrReplaceTempView('insights')
                spark.sql(f"""
                        INSERT INTO gpc_{tenant}.raw_transcript_insights (additional_service, process_knowledge, conversation_id, contact, system_knowledge, process_map, satisfaction, resolved, extractDate, extractIntervalStartTime, extractIntervalEndTime, recordInsertTime, recordIdentifier)
                        SELECT additional_service, process_knowledge, conversation_id, contact, system_knowledge, process_map, satisfaction, resolved,CAST('{extract_start_time}' AS DATE) extractDate, '{extract_start_time}' extractIntervalStartTime, '{extract_end_time}' extractIntervalEndTime, '{datetime.now()}' recordInsertTime,
                        1 recordIdentifier  FROM insights
                        """)
                            
                insert_insights_audit(spark, tenant, True,conv['conversationId'] ,response.status_code, size,startTime, endTime, conv['conversationStartDateId'],'Success',urldisplay)

            except Exception as e:
                endTime = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                logger.exception(f"Error Occurred in insights insertion for conversation: {conv['conversationId']}")
                logger.exception(e, stack_info=True, exc_info=True)
                insert_insights_audit(spark, tenant, False,conv['conversationId'],500, size,startTime,endTime,conv['conversationStartDateId'],e,urldisplay)
        
        
def pool_executor(spark, conversations, url, tenant, extract_start_time, extract_end_time, schema, token):    
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(process_conversation, spark, conv, url, tenant, extract_start_time, extract_end_time, schema, token) for conv in conversations.rdd.collect()]
        # print("Total processed conversations With this thread: ", len(futures))

def get_transcript_insights(spark: SparkSession, tenant: str, run_id: str, extract_start_time: str, extract_end_time: str):
    logger = helios_utils_logger(tenant, "transcript_insights")
    logger.info(f'extract_start_time : {extract_start_time}')
    logger.info(f'extract_end_time : {extract_end_time}')
    conversations = get_conversations(spark, tenant, extract_start_time, extract_end_time)

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
    schema = get_api_schema()
    token = getAccessToken()

    # Creating threads to process each half in parallel
    thread1 = threading.Thread(target=pool_executor, args=(spark, first_half_conversations, url1, tenant, extract_start_time, extract_end_time, schema, token))
    thread2 = threading.Thread(target=pool_executor, args=(spark, second_half_conversations, url2, tenant, extract_start_time, extract_end_time, schema, token))

    # Start the threads
    thread1.start()
    thread2.start()

    # Wait for both threads to complete
    thread1.join()
    thread2.join()