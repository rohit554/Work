import json
import requests as requests
import time
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import (
    authorize,
    get_api_url,
    process_raw_data,
    gpc_utils_logger,
)
from pyspark.sql.functions import col
from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
import os
import pandas as pd
from socket import error as SocketError
import errno
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import dganalytics.connectors.gpc.batch.etl.extract_api.speechandtextanalytics_transcript as C


def get_conversations(
    spark: SparkSession, tenant: str, extract_start_time: str, extract_end_time: str
):
    if tenant == "hellofresh":
        tenant_path, db_path, log_path = get_path_vars(tenant)
        user_timezones = pd.read_json(
            os.path.join(
                tenant_path, "data", "config", "DG_Agent_Group_Site_Timezone.json"
            )
        )
        user_timezones = pd.DataFrame(user_timezones["values"].tolist())
        header = user_timezones.iloc[0]
        user_timezones = user_timezones[1:]
        user_timezones.columns = header
        user_timezones = spark.createDataFrame(user_timezones)
        user_timezones.createOrReplaceTempView("user_timezones")

        queue_language = pd.read_json(
            os.path.join(tenant_path, "data", "config", "outboundQueueLanguage.json")
        )
        queue_language = pd.DataFrame(queue_language["values"].tolist())
        header = queue_language.iloc[0]
        queue_language = queue_language[1:]
        queue_language.columns = header
        queue_language = spark.createDataFrame(queue_language)
        queue_language.createOrReplaceTempView("queue_language")

        queue_language = spark.sql(
            """select ql.Region,ql.Brand,ql.Queues as queueName,ql.`Language ` as `language`,r.queueId
                                    from queue_language ql
                                JOIN gpc_hellofresh.dim_routing_queues r ON ql.Queues = r.queueName
                                """
        )
        queue_language.write.mode("overwrite").saveAsTable(
            "gpc_hellofresh.dim_queue_language"
        )

    conversations_df = C.get_conversations(
        spark, tenant, extract_start_time, extract_end_time
    )
    conversations_df = conversations_df.withColumnRenamed(
        "conversationId", "conversation_id"
    )
    fact_df = spark.table(f"gpc_{tenant}.fact_speechandtextanalytics").select("conversationId")
    filtered_conversations_df = conversations_df.join(
        fact_df,
        conversations_df["conversation_id"] == fact_df["conversationId"],
        "left_anti"
    )
    return filtered_conversations_df


def get_speechandtextanalytics(
    base_url: str, auth_headers, conversation_id: str, retry_count: int
):
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
            return {}

        logger.info(
            f"Rate limit exceeded for get Speech And Text Analytics API call, sleeping for 30 seconds, retry count {retry_count} of 3"
        )
        time.sleep(30)

        return get_speechandtextanalytics(
            base_url, auth_headers, conversation_id, retry_count
        )
    elif resp.status_code == 404:
        logger.info(
            f"No Speech And Text Analytics found for conversation {conversation_id}"
        )
        return {}
    elif resp.status_code != 200:
        logger.error(resp)
        return {}

    return resp.json()


def exec_speechandtextanalytics(
    spark: SparkSession,
    tenant: str,
    run_id: str,
    extract_start_time: str,
    extract_end_time: str,
):
    global logger
    logger = gpc_utils_logger(tenant, "gpc_speechandtextanalytics")
    logger.info(
        f"getting conversations extracted between {extract_start_time} and {extract_end_time} for Speech And Text Analytics"
    )

    conversations_df = get_conversations(
        spark, tenant, extract_start_time, extract_end_time
    )

    if conversations_df is None or not hasattr(conversations_df, "rdd"):
        logger.error(
            "Failed to retrieve valid conversations DataFrame. Exiting thread execution."
        )
        return

    auth_headers = authorize(tenant)
    base_url = get_api_url(tenant)
    speechandtextanalytics = []

    valid_conversations = conversations_df.filter(
        conversations_df["conversation_id"].isNotNull()
    )

    with ThreadPoolExecutor(max_workers=100) as executor:
        results = executor.map(
            lambda conversation: get_speechandtextanalytics(
                base_url, auth_headers, conversation["conversation_id"], 0
            ),
            valid_conversations.rdd.collect(),
        )

    for resp_json in results:
        if resp_json is not None and len(resp_json) > 0:
            logger.info(
                f"adding {len(resp_json)} Speech And Text Analytics for a conversation"
            )
            speechandtextanalytics.append(resp_json)

    if len(speechandtextanalytics) > 0:
        process_raw_data(
            spark,
            tenant,
            "speechandtextanalytics",
            run_id,
            speechandtextanalytics,
            extract_start_time,
            extract_end_time,
            len(speechandtextanalytics),
        )