import json
import os
import sys
import threading
import concurrent.futures
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType,
    IntegerType, TimestampType, DateType
)

from dganalytics.utils.utils import get_secret, get_path_vars
from dganalytics.helios.helios_utils import helios_utils_logger


def get_api_schema(api_name: str, connector: str) -> StructType:
    """
    Retrieve the API schema from a JSON file and convert it to a Spark StructType.

    Args:
        api_name (str): The name of the API.
        connector (str): The connector name used to locate the schema file.

    Returns:
        StructType: The schema defined in the JSON file.
    """
    logger1 = gpc_utils_logger(connector, api_name)
    schema_path = os.path.join(
        Path(__file__).parent.parent.parent,
        'connectors',
        connector,
        'source_api_schemas',
        f'{api_name}.json'
    )
    with open(schema_path, 'r') as f:
        schema_json = json.load(f)
    return StructType.fromJson(schema_json)


def get_access_token() -> str:
    """
    Obtain an access token by authenticating with the AWS Lambda function.

    Returns:
        str: The access token retrieved from the authentication response.

    Raises:
        Exception: If the authentication request fails.
    """
    url = get_secret("awsLambdaAuthURL")
    payload = {
        "user_id": get_secret("awsLambdaAuthUserId"),
        "user_password": get_secret("awsLambdaAuthPassword")
    }
    headers = {
        'Content-Type': 'application/json'
    }

    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
    except requests.RequestException as e:
        raise Exception("Failed to obtain access token") from e

    return response.json().get('data')


def get_conversation_transcript(
    spark: SparkSession,
    tenant: str,
    extract_start_time: str,
    extract_end_time: str,
    conversation_id: str
):
    """
    Retrieve the transcript of a specific conversation from the Spark SQL database.

    Args:
        spark (SparkSession): The Spark session.
        tenant (str): The tenant identifier.
        extract_start_time (str): The start time for extraction in 'yyyyMMdd' format.
        extract_end_time (str): The end time for extraction.
        conversation_id (str): The ID of the conversation to retrieve.

    Returns:
        Row or None: The first row of the result containing conversation details, or None if not found.
    """
    query = f"""
        SELECT 
            conversationId, 
            concat_ws("\\n", collect_list(phrase)) AS conversation, 
            conversationStartDateId
        FROM (
            SELECT 
                P.conversationId, 
                concat(
                    string(row_number() OVER(PARTITION BY P.conversationId ORDER BY P.startTimeMs)), 
                    ':', 
                    P.participantPurpose, 
                    ':', 
                    text
                ) AS phrase, 
                C.conversationStartDateId 
            FROM gpc_{tenant}.fact_conversation_transcript_phrases P
            INNER JOIN dgdm_{tenant}.dim_conversations C
                ON P.conversationId = C.conversationId
            WHERE P.conversationId = '{conversation_id}'
        )
        GROUP BY conversationId, conversationStartDateId
    """
    try:
        df = spark.sql(query).collect()
        if df:
            return df[0]
    except Exception as e:
        logger = helios_utils_logger(tenant, "transcript_insights")
        logger.error(f"Error fetching transcript for conversation {conversation_id}: {e}")
    return None


def insert_insights_audit(
    spark: SparkSession,
    tenant: str,
    status: bool,
    conversation_id: str,
    error_or_status_code: str,
    transcript_size: int,
    processing_start_time: datetime,
    processing_end_time: datetime,
    conversation_start_date_id: int,
    error: str,
    url: str
):
    """
    Insert an audit record into the transcript_insights_audit table.

    Args:
        spark (SparkSession): The Spark session.
        tenant (str): The tenant identifier.
        status (bool): The status of the processing (True for success, False otherwise).
        conversation_id (str): The ID of the conversation.
        error_or_status_code (str): The error message or status code.
        transcript_size (int): The size of the transcript.
        processing_start_time (datetime): Start time of processing.
        processing_end_time (datetime): End time of processing.
        conversation_start_date_id (int): The start date ID of the conversation.
        error (str): Error details if any.
        url (str): The URL used for processing.
    """
    insert_query = f"""
        INSERT INTO dgdm_{tenant}.transcript_insights_audit 
        VALUES (
            {int(status)},
            '{conversation_id}',
            '{error_or_status_code}',
            {transcript_size},
            '{processing_start_time}',
            '{processing_end_time}',
            {conversation_start_date_id},
            '{error}',
            '{url}',
            current_timestamp()
        )
    """
    try:
        spark.sql(insert_query)
    except Exception as e:
        logger = helios_utils_logger(tenant, "transcript_insights")
        logger.error(f"Failed to insert audit record for conversation {conversation_id}: {e}")


def get_conversations(
    spark: SparkSession,
    tenant: str,
    extract_start_time: str,
    extract_end_time: str
):
    """
    Retrieve a distinct list of conversation IDs that need to be processed.

    Args:
        spark (SparkSession): The Spark session.
        tenant (str): The tenant identifier.
        extract_start_time (str): The start time for extraction in 'yyyyMMdd' format.
        extract_end_time (str): The end time for extraction.

    Returns:
        DataFrame: A Spark DataFrame containing distinct conversation IDs.
    """
    query = f"""
        SELECT DISTINCT P.conversationId 
        FROM gpc_{tenant}.fact_conversation_transcript_phrases P
		JOIN gpc_hellofresh.raw_speechandtextanalytics_transcript T
          ON T.extractDate = date_format('{extract_start_time}', 'yyyy-MM-dd')
          and P.conversationId = T.conversationId
        INNER JOIN dgdm_{tenant}.dim_conversations C
            ON P.conversationId = C.conversationId
        WHERE (C.conversationStartDateId = date_format('{extract_start_time}', 'yyyyMMdd')
				OR C.conversationStartDateId between date_format(DATE_SUB('{extract_start_time}', 34), 'yyyyMMdd')  and date_format('{extract_start_time}', 'yyyyMMdd'))
            AND NOT EXISTS (
                SELECT 1  
                FROM dgdm_{tenant}.fact_transcript_insights i
                WHERE i.conversationStartDateId = date_format('{extract_start_time}', 'yyyyMMdd')
                    AND i.conversationId = P.conversationId
            )
    """
    try:
        return spark.sql(query)
    except Exception as e:
        logger = helios_utils_logger(tenant, "transcript_insights")
        logger.error(f"Error retrieving conversations: {e}")
        return spark.createDataFrame([], StructType([]))


def send_to_lambda(
    payload: dict,
    headers: dict,
    url: str,
    timeout: int = 180
) -> requests.Response:
    """
    Send a POST request to the AWS Lambda function.

    Args:
        payload (dict): The JSON payload to send.
        headers (dict): The HTTP headers.
        url (str): The Lambda function URL.
        timeout (int, optional): The request timeout in seconds. Defaults to 180.

    Returns:
        requests.Response: The HTTP response.

    Raises:
        requests.RequestException: If the request fails.
    """
    return requests.post(url, headers=headers, json=payload, timeout=timeout)


def process_conversation(
    spark: SparkSession,
    conv: dict,
    region: str,
    tenant: str,
    extract_start_time: str,
    extract_end_time: str,
    schema: StructType,
    token: str,
    lambda_url: str
):
    """
    Process a single conversation by fetching its transcript, sending it to the Lambda function,
    and handling the response or any errors.

    Args:
        spark (SparkSession): The Spark session.
        conv (dict): The conversation dictionary containing conversationId and other details.
        region (str): The AWS region to use for the Lambda function.
        tenant (str): The tenant identifier.
        extract_start_time (str): The start time for extraction.
        extract_end_time (str): The end time for extraction.
        schema (StructType): The schema for the insights data.
        token (str): The access token for authentication.
        lambda_url (str): The AWS Lambda function URL.
    """
    conversation_id = conv['conversationId']
    logger = helios_utils_logger(tenant, "transcript_insights")
    processing_start_time = datetime.utcnow()

    # Retrieve the conversation transcript
    conversation = get_conversation_transcript(
        spark, tenant, extract_start_time, extract_end_time, conversation_id
    )

    if not conversation:
        logger.warning(f"No transcript found for conversation {conversation_id}")
        return

    transcript = conversation['conversation']
    transcript_size = sys.getsizeof(transcript)

    if transcript_size < 1024:
        logger.warning(f"Small conversation: {conversation_id}")
        insert_insights_audit(
            spark=spark,
            tenant=tenant,
            status=False,
            conversation_id=conversation_id,
            error_or_status_code="400",
            transcript_size=transcript_size,
            processing_start_time=processing_start_time,
            processing_end_time=datetime.utcnow(),
            conversation_start_date_id=conversation['conversationStartDateId'],
            error='Small Conversation',
            url=region
        )
        return

    headers = {
        'dg-token': token,
        'Content-Type': 'application/json'
    }

    payload = {
        "conversation_id": conversation_id,
        "conversation": transcript,
        "tenant_id": tenant,
        "call_type": "outbound",
        "bedrock_region": region,
        "process_insight": 1,
        "process_action": 1,
        "process_quality": 1,
        "process_compliance": 1,
        "process_summary": 1
    }

    max_retries = 3
    retry_count = 0
    response = None

    while retry_count < max_retries:
        try:
            response = send_to_lambda(payload, headers, lambda_url)
            if response.status_code == 200:
                break
        except requests.Timeout:
            logger.error(f"Timeout for conversation: {conversation_id}")
            insert_insights_audit(
                spark=spark,
                tenant=tenant,
                status=False,
                conversation_id=conversation_id,
                error_or_status_code="408",
                transcript_size=transcript_size,
                processing_start_time=processing_start_time,
                processing_end_time=datetime.utcnow(),
                conversation_start_date_id=conversation['conversationStartDateId'],
                error='Conversation Timed Out',
                url=region
            )
            return
        except requests.RequestException as e:
            logger.error(f"Request exception for conversation {conversation_id}: {e}")
            insert_insights_audit(
                spark=spark,
                tenant=tenant,
                status=False,
                conversation_id=conversation_id,
                error_or_status_code="500",
                transcript_size=transcript_size,
                processing_start_time=processing_start_time,
                processing_end_time=datetime.utcnow(),
                conversation_start_date_id=conversation['conversationStartDateId'],
                error=str(e),
                url=region
            )
            return
        retry_count += 1

    if response and response.status_code == 200:
        try:
            data = response.json().get('data', {})
            insights = spark.createDataFrame([data], schema=schema)
            insights.createOrReplaceTempView('insights')

            insert_query = f"""
                INSERT INTO gpc_{tenant}.raw_transcript_insights (
                    conversation_id, contact, scoring, sentiment,
                    resolved, satisfaction, additional_service, process_knowledge, system_knowledge,
                    process_map, quality, compliance, summary, extractDate, 
                    extractIntervalStartTime, extractIntervalEndTime, recordInsertTime, recordIdentifier
                )
                SELECT 
                    conversation_id, contact, scoring, sentiment, resolved, satisfaction, 
                    additional_service, process_knowledge, system_knowledge,
                    process_map, quality, compliance, summary, 
                    CAST('{extract_start_time}' AS DATE) AS extractDate, 
                    CAST('{extract_start_time}' AS TIMESTAMP) AS extractIntervalStartTime, 
                    CAST('{extract_end_time}' AS TIMESTAMP) AS extractIntervalEndTime, 
                    current_timestamp() AS recordInsertTime, 
                    1 AS recordIdentifier  
                FROM insights
            """
            spark.sql(insert_query)

            insert_insights_audit(
                spark=spark,
                tenant=tenant,
                status=True,
                conversation_id=conversation_id,
                error_or_status_code=str(response.status_code),
                transcript_size=transcript_size,
                processing_start_time=processing_start_time,
                processing_end_time=datetime.utcnow(),
                conversation_start_date_id=conversation['conversationStartDateId'],
                error='Success',
                url=region
            )
        except Exception as e:
            logger.error(f"Error inserting insights for conversation {conversation_id}: {e}")
            insert_insights_audit(
                spark=spark,
                tenant=tenant,
                status=False,
                conversation_id=conversation_id,
                error_or_status_code="500",
                transcript_size=transcript_size,
                processing_start_time=processing_start_time,
                processing_end_time=datetime.utcnow(),
                conversation_start_date_id=conversation['conversationStartDateId'],
                error=str(e),
                url=region
            )
    else:
        logger.error(f"Failed to process conversation {conversation_id} after {max_retries} retries")
        insert_insights_audit(
            spark=spark,
            tenant=tenant,
            status=False,
            conversation_id=conversation_id,
            error_or_status_code=str(response.status_code) if response else "No Response",
            transcript_size=transcript_size,
            processing_start_time=processing_start_time,
            processing_end_time=datetime.utcnow(),
            conversation_start_date_id=conversation['conversationStartDateId'],
            error='Failed after retries',
            url=region
        )


def pool_executor(
    spark: SparkSession,
    conversations: SparkSession,
    region: str,
    tenant: str,
    extract_start_time: str,
    extract_end_time: str,
    schema: StructType,
    token: str,
    lambda_url: str,
    max_workers: int = 3
):
    """
    Execute the processing of conversations using a thread pool.

    Args:
        spark (SparkSession): The Spark session.
        conversations (DataFrame): The DataFrame containing conversations to process.
        region (str): The AWS region URL for the Lambda function.
        tenant (str): The tenant identifier.
        extract_start_time (str): The start time for extraction.
        extract_end_time (str): The end time for extraction.
        schema (StructType): The schema for the insights data.
        token (str): The access token for authentication.
        lambda_url (str): The AWS Lambda function URL.
        max_workers (int, optional): Maximum number of worker threads. Defaults to 3.
    """
    conversation_list = conversations.rdd.map(lambda row: row.asDict()).collect()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_conv = {
            executor.submit(
                process_conversation,
                spark,
                conv,
                region,
                tenant,
                extract_start_time,
                extract_end_time,
                schema,
                token,
                lambda_url
            ): conv for conv in conversation_list
        }
        for future in concurrent.futures.as_completed(future_to_conv):
            conv = future_to_conv[future]
            try:
                future.result()
            except Exception as e:
                logger = helios_utils_logger(tenant, "transcript_insights")
                logger.error(f"Unhandled exception processing conversation {conv['conversationId']}: {e}")


def get_transcript_insights(
    spark: SparkSession,
    tenant: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str
):
    """
    Main function to orchestrate the retrieval and processing of transcript insights.

    Args:
        spark (SparkSession): The Spark session.
        tenant (str): The tenant identifier.
        run_id (str): The run identifier.
        extract_start_time (str): The start time for extraction.
        extract_end_time (str): The end time for extraction.
    """
    logger = helios_utils_logger(tenant, "transcript_insights")
    logger.info(f"Extract Start Time: {extract_start_time}")
    logger.info(f"Extract End Time: {extract_end_time}")

    regions = [
        'us-east-1',
        'us-west-2',
        'ap-southeast-2',
        'ap-south-1',
        'eu-central-1',
        'eu-west-2',
        'eu-west-3',
        'ca-central-1',
        'sa-east-1'
    ]

    conversations_df = get_conversations(spark, tenant, extract_start_time, extract_end_time)
    total_conversations = conversations_df.count()
    logger.info(f"Total Conversations to process: {total_conversations}")

    if total_conversations == 0:
        logger.info("No conversations to process.")
        return
    
    if tenant in ['hellofresh', 'salmatcolesonline']:
        schema = get_api_schema('transcript_insights', 'gpc')
    else:
        schema = get_api_schema('transcript_insights', 'gpc_v2')
    # Retrieve the AWS Lambda function URL from secrets.
    lambda_url = 'https://zhwvcwlgcp65drv4h4lm7yzzs40mlupl.lambda-url.us-east-1.on.aws/' # get_secret("awsLambdaFunctionURL") # ///TODO: add to secrets
    token = get_access_token()

    num_parts = len(regions)
    part_size = total_conversations // num_parts
    conversations_list = []

    copy_df = conversations_df
    for i in range(num_parts):
        if i < num_parts - 1:
            temp_df = copy_df.limit(part_size)
        else:
            temp_df = copy_df
        conversations_list.append(temp_df)
        copy_df = copy_df.subtract(temp_df)

    threads = []
    for i, region in enumerate(regions):
        if i >= len(conversations_list):
            logger.warning(f"No conversations assigned to region {region}")
            continue
        thread = threading.Thread(
            target=pool_executor,
            args=(
                spark,
                conversations_list[i],
                region,
                tenant,
                extract_start_time,
                extract_end_time,
                schema,
                token,
                lambda_url
            )
        )
        threads.append(thread)
        thread.start()
        logger.info(f"Started thread for region {region} with {conversations_list[i].count()} conversations")

    for thread in threads:
        thread.join()

    logger.info("All conversations processed.")
