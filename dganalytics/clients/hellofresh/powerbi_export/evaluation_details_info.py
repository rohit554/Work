from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_evaluation_details_info(sapark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping_v2.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_mapping = spark.createDataFrame(queue_timezones)
    queue_mapping.createOrReplaceTempView("queue_mapping")

    df = spark.sql(f"""
           SELECT 
            a.agentId AS agentKey,
            a.agentHasRead AS agentHasRead,
            a.anyFailedKillQuestions AS anyFailedKillQuestions,
            c.totalCriticalScore AS totalCriticalScore,
            c.totalNonCriticalScore AS totalNonCriticalScore,
            c.totalScore AS totalScore,
            from_utc_timestamp(a.assignedDate, trim(e.timeZone)) AS assignedDate,
            from_utc_timestamp(a.changedDate, trim(e.timeZone)) AS changedDate,
            a.conversationId AS conversationKey,
            from_utc_timestamp(a.conversationDate, trim(e.timeZone)) AS conversationDate,
            a.evaluationFormId AS evaluationFormKey,
            b.evaluationFormName AS evaluationFormName,
            a.evaluationFormPublished AS evaluationFormPublished,
            a.evaluatorId AS evaluatorKey,
            a.evaluationId AS evaluationKey,
            a.mediaType AS mediaType,
            a.neverRelease AS neverRelease,
            from_utc_timestamp(a.releaseDate, trim(e.timeZone)) AS releaseDate,
            a.status AS status,
            a.queueId AS queueKey,
            a.wrapUpCode
        FROM 
            gpc_hellofresh.dim_evaluations a, 
            gpc_hellofresh.dim_evaluation_forms b,
            gpc_hellofresh.fact_evaluation_total_scores c,
            gpc_hellofresh.dim_routing_queues d, 
            queue_mapping e
        WHERE 
            a.evaluationFormId = b.evaluationFormId
            AND CAST(from_utc_timestamp(a.conversationDate, trim(e.timeZone)) AS date) >= date_sub(current_date, 365)
            AND a.evaluationId = c.evaluationId
            AND a.conversationDatePart = c.conversationDatePart
            AND a.queueId = d.queueId
            AND d.queueName = e.queueName
    """)

    return df
