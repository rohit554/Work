import json
import requests as requests
import time
from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.functions import col
import time
from requests.exceptions import RequestException

from dganalytics.connectors.gpc.gpc_utils import (
    authorize,
    get_api_url,
    process_raw_data,
)
from dganalytics.connectors.gpc.gpc_utils import gpc_utils_logger
from datetime import datetime, timedelta, date


def calculate_week_ranges(today):
    # Calculate the end of this week
    this_week_end = today + timedelta(days=6)

    # Calculate start and end of the last week
    last_week_end = today - timedelta(days=1)
    last_week_start = last_week_end - timedelta(days=6)

    # Initialize the results with default values
    last_month_week_start = last_week_start
    last_month_week_end = last_week_end
    this_month_last_week_start = last_week_start
    this_month_last_week_end = last_week_end

    # Check if the last week was in the previous month
    if last_week_start.month != last_week_end.month:
        # Last week spans over two months
        if last_week_end.month != today.month:
            # Last week was entirely in the previous month
            this_month_last_week_start = None
            this_month_last_week_end = None
        else:
            # Part of the last week was in the previous month
            last_month_week_end = date(
                last_week_end.year, last_week_end.month, 1
            ) - timedelta(days=1)
            this_month_last_week_start = date(
                last_week_end.year, last_week_end.month, 1
            )
            this_month_last_week_end = last_week_end
    else:
        # Last week was entirely in the current month
        if today.day >= 8:
            last_month_week_start = None
            last_month_week_end = None
        if last_week_end.month != today.month:
            # Last week was entirely in the previous month
            this_month_last_week_start = None
            this_month_last_week_end = None
    print(
        this_month_last_week_start,
        this_month_last_week_end,
        last_month_week_start,
        last_month_week_end,
    )
    return (
        this_month_last_week_start,
        this_month_last_week_end,
        last_month_week_start,
        last_month_week_end,
    )


def get_failure_interactions(spark, tenant, start, end):
    # start = datetime.strptime(start, '%Y-%m-%d')
    # end = datetime.strptime(end, '%Y-%m-%d')
    # Get the first day of the month
    month_start = date(start.year, start.month, 1)

    # Get the last day of the month by finding the first day of the next month and subtracting one day
    if start.month == 12:
        month_end = date(start.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(start.year, start.month + 1, 1) - timedelta(days=1)

    users_df = spark.sql(
        f"""SELECT c.userId, count(distinct i.conversation_id) failed_conversation_count
                            FROM gpc_{tenant}.raw_transcript_insights i
                            JOIN gpc_{tenant}.dim_last_handled_conversation c
                                ON i.conversation_id = c.conversationId
                                    AND i.extractDate BETWEEN '{month_start}' AND DATE_ADD('{month_end}', 3)
                                    AND c.conversationStartDate BETWEEN '{month_start}' AND '{month_end}'
                                    AND c.wrapUpCodeId = 'c0ad6c2a-a0fa-42c5-97c3-2e2d49712564'
                            GROUP BY c.userId
        """
    )

    users_df.createOrReplaceTempView("users_df")

    return spark.sql(
        f"""
                WITH failures AS (
                SELECT 
                    conversationId, 
                    userId, 
                    sessionId AS communicationId,
                    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY conversationStart DESC) as rk
                FROM (
                    SELECT 
                        conversationId, 
                        userId, 
                        sessions.sessionId AS sessionId,
                        conversationStart,
                        explode(sessions.segments) AS segment
                    FROM (
                        SELECT 
                            conversationId, 
                            userId, 
                            participants.participantId, 
                            participants.purpose, 
                            sessions.sessionId,
                            conversationStart,
                            explode(participants.sessions) AS sessions
                        FROM (
                            SELECT 
                                conversationId, 
                                userId, 
                                conversationStart, 
                                explode(participants) AS participants
                            FROM (
                                SELECT 
                                    b.conversationId, 
                                    b.userId, 
                                    b.conversationStart, 
                                    d.participants,
                                    ROW_NUMBER() OVER (PARTITION BY d.conversationId ORDER BY d.recordInsertTime DESC) AS rn
                                FROM (
                                    SELECT 
                                        a.conversationId, 
                                        a.userId, 
                                        a.conversationStart
                                    FROM gpc_{tenant}.dim_last_handled_conversation a
                                    JOIN dgdm_{tenant}.dim_queue_language ql 
                                        ON ql.queueId = a.queueId
                                    JOIN gpc_{tenant}.dim_wrapup_codes wc
                                        ON wc.wrapupId = a.wrapUpCodeId
                                    JOIN gpc_{tenant}.fact_conversation_metrics c
                                        ON a.conversationId = c.conversationId
                                        AND a.conversationStartDate = c.emitDate
                                    WHERE a.mediaType  in ('voice', 'callback')
                                        AND c.emitDate BETWEEN cast('{start}' as DATE) AND CAST('{end}' AS DATE)
                                        AND c.tTalkComplete > 60
                                        AND ql.language = 'English'
                                        AND wc.wrapupCode IN ('Z-OUT-Failure')
										AND NOT EXISTS (SELECT 1 FROM gpc_{tenant}.raw_speechandtextanalytics_transcript T
                                WHERE T.conversationId = a.conversationId
                                AND T.extractDate = cast('{start}' as DATE) )
                                ) AS b
                                JOIN gpc_{tenant}.raw_conversation_details d 
                                    ON b.conversationId = d.conversationId
                                WHERE d.extractDate BETWEEN cast('{start}' as DATE) AND CAST('{end}' AS DATE)
                            ) WHERE rn = 1
                        ) 
                        WHERE participants.purpose IN ('customer', 'external')
                    )
                ) 
                WHERE segment.segmentType = 'dialing'
                )

                SELECT f.conversationId, f.communicationId
                FROM failures f
                LEFT JOIN users_df u
                    ON f.userId = u.userId
                WHERE COALESCE(u.failed_conversation_count, 0) < 5
                AND f.rk <= (5 - COALESCE(u.failed_conversation_count, 0))
            """
    )


def get_conversations(
    spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str
):
    if tenant == "hellofresh":
        conversation_df = spark.sql(
            f"""
            SELECT conversationId, sessions.sessionId AS communicationId 
            FROM (
                SELECT conversationId, userId, conversationStart, explode(participants.sessions) as sessions 
                FROM (
                    SELECT conversationId, userId, conversationStart, explode(participants) as participants 
                    FROM (
                        SELECT b.conversationId, b.userId, b.conversationStart, d.participants,
                        ROW_NUMBER() OVER(PARTITION BY d.conversationId ORDER BY d.recordInsertTime DESC) AS rn
                        FROM (
                            SELECT a.conversationId, a.userId, a.conversationStart
                            FROM gpc_{tenant}.dim_last_handled_conversation a
                            JOIN dgdm_{tenant}.dim_queue_language ql 
                                ON ql.queueId = a.queueId
                            JOIN gpc_{tenant}.dim_wrapup_codes wc
                                ON wc.wrapupId = a.wrapUpCodeId
                            JOIN gpc_{tenant}.fact_conversation_metrics c
                                ON a.conversationId = c.conversationId
                            WHERE a.mediaType in ('voice', 'callback')
                                AND c.emitDate BETWEEN (DATE_SUB('{extract_start_time}', 2)) AND '{extract_end_time}'
                                AND c.tTalkComplete > 60
                                AND ql.language = 'English'
                                AND wc.wrapupCode IN (
                                    'Z-OUT-Success-1Week', 
                                    'Z-OUT-Success-2Week', 
                                    'Z-OUT-Success-3Week', 
                                    'Z-OUT-Success-4Week', 
                                    'Z-OUT-Success-5+Week-TL APPROVED'
                                )
								AND NOT EXISTS (SELECT 1 FROM gpc_{tenant}.raw_speechandtextanalytics_transcript T
                                WHERE T.conversationId = a.conversationId
                                AND T.extractDate between cast('{extract_start_time}' as DATE) AND cast('{extract_end_time}' as DATE) )
                        ) AS b
                        JOIN gpc_{tenant}.raw_conversation_details d 
                            ON b.conversationId = d.conversationId
                        WHERE d.extractDate between cast('{extract_start_time}' as DATE) AND cast('{extract_end_time}' as DATE)
                    )
                    WHERE rn = 1
                )
                WHERE participants.purpose IN ('customer', 'external')
            )
			WHERE sessions.mediaType = 'voice'
        """
        )

        today = datetime.strptime(extract_start_time, "%Y-%m-%dT%H:%M:%S")

        # Runs every sunday at 00:00:00
        if today.weekday() == 6 and today.hour == 0:
            (
                this_month_last_week_start,
                this_month_last_week_end,
                last_month_week_start,
                last_month_week_end,
            ) = calculate_week_ranges(today)

            if this_month_last_week_start is not None:
                print("in this month")
                failure_df = get_failure_interactions(spark,
                    tenant, this_month_last_week_start, this_month_last_week_end
                )
                conversation_df = conversation_df.union(failure_df)
            if last_month_week_start is not None:
                print("in last month")
                failure_df = get_failure_interactions(spark,
                    tenant, last_month_week_start, last_month_week_end
                )
                conversation_df = conversation_df.union(failure_df)

    else:
        conversation_df = spark.sql(
            f"""
            SELECT DISTINCT C.conversationId, C.sessionId AS communicationId
            FROM gpc_{tenant}.raw_speechandtextanalytics RS
            JOIN (
                SELECT conversationId, sessions.sessionId AS sessionId
                FROM (
                    SELECT conversationId, EXPLODE(participants.sessions) AS sessions
                    FROM (
                        SELECT conversationId, EXPLODE(participants) AS participants
                        FROM gpc_{tenant}.raw_conversation_details
                    )
                )
                WHERE participants.purpose IN ('customer', 'external')
            ) C
            ON RS.conversation.id = C.conversationId
            WHERE extractIntervalStartTime = '{extract_start_time}'
              AND extractIntervalEndTime = '{extract_end_time}'
        """
        )

    return conversation_df

def fetch_transcript_url(url, headers, max_retries=3, retry_delay=5):
    """
    Fetch the transcript URL with proper error handling and retry logic.
    """
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, headers=headers)

            # Handle rate limiting (HTTP 429)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", retry_delay))
                logger.warning(
                    f"Rate limit hit. Retrying after {retry_after} seconds. Attempt {attempt + 1}/{max_retries}."
                )
                time.sleep(retry_after)
                continue

            # Handle other non-200 responses
            if resp.status_code != 200:
                logger.error(
                    f"Failed to fetch transcript URL. Status code: {resp.status_code}, Response: {resp.text}"
                )
                return None

            # Return the URL if successful
            return resp.json().get("url")

        except RequestException as e:
            logger.exception(
                f"Request failed due to an exception: {e}. Attempt {attempt + 1}/{max_retries}."
            )
            time.sleep(retry_delay)

    logger.error(f"Failed to fetch transcript URL after {max_retries} attempts.")
    return None


def fetch_transcript_data(url, max_retries=3, retry_delay=30):
    """
    Fetch the transcript data with proper error handling and retry logic.
    """
    for attempt in range(max_retries):
        try:
            resp = requests.get(url)

            # Handle rate limiting (HTTP 429)
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", retry_delay))
                logger.warning(
                    f"Rate limit hit. Retrying after {retry_after} seconds. Attempt {attempt + 1}/{max_retries}."
                )
                time.sleep(retry_after)
                continue

            # Handle other non-200 responses
            if resp.status_code != 200:
                logger.error(
                    f"Failed to fetch transcript data. Status code: {resp.status_code}, Response: {resp.text}"
                )
                return None

            # Return the transcript data if successful
            return resp.json()

        except RequestException as e:
            logger.exception(
                f"Request failed due to an exception: {e}. Attempt {attempt + 1}/{max_retries}."
            )
            time.sleep(retry_delay)

    logger.error(f"Failed to fetch transcript data after {max_retries} attempts.")
    return None

def exec_speechandtextanalytics_transcript(
    spark: SparkSession,
    tenant: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str,
):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_speechandtextanalytics_transcript")
    base_url = get_api_url(tenant)
    auth_headers = authorize(tenant)

    logger.info(
        f"Getting conversationId and communicationId extracted for {extract_start_time} to {extract_end_time} for Speech And Text Analytics Transript"
    )

    conversations = get_conversations(
        spark, tenant, extract_start_time, extract_end_time
    )
    convs_df = conversations.toPandas()
    transcripts = []

    def fetch_transcript(conversation):
        """
        Fetch transcript data for a single conversation.
        """
        conversation_id = conversation.conversationId
        communication_id = conversation.communicationId

        url = f"{base_url}/api/v2/speechandtextanalytics/conversations/{conversation_id}/communications/{communication_id}/transcripturl"
        transcript_url = fetch_transcript_url(url, auth_headers)
        if not transcript_url:
            return None

        # Fetch transcript data
        return fetch_transcript_data(transcript_url)

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_conversation = {
            executor.submit(fetch_transcript, conversation): conversation
            for conversation in conversations.select("conversationId", "communicationId").rdd.collect()
        }

        for future in as_completed(future_to_conversation):
            conversation = future_to_conversation[future]
            try:
                transcript_data = future.result()
                if transcript_data:
                    transcripts.append(json.dumps(transcript_data))
            except Exception as e:
                logger.error(
                    f"Error processing conversation {conversation.conversationId}: {e}"
                )

    if len(transcripts) > 0:
        process_raw_data(
            spark,
            tenant,
            "speechandtextanalytics_transcript",
            run_id,
            transcripts,
            extract_start_time,
            extract_end_time,
            len(transcripts),
        )