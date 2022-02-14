from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_evaluation_details_info(spark: SparkSession, tenant: str, region: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_mapping = spark.createDataFrame(queue_timezones)
    queue_mapping.createOrReplaceTempView("queue_mapping")

    df = spark.sql(f"""
           select 
a.agentId agentKey,
a.agentHasRead agentHasRead,
a.anyFailedKillQuestions anyFailedKillQuestions,
c.totalCriticalScore totalCriticalScore,
c.totalNonCriticalScore totalNonCriticalScore,
c.totalScore totalScore,
from_utc_timestamp(a.assignedDate, trim(e.timeZone)) assignedDate,
from_utc_timestamp(a.changedDate, trim(e.timeZone)) changedDate,
a.conversationId conversationKey,
from_utc_timestamp(a.conversationDate, trim(e.timeZone)) conversationDate,
a.evaluationFormId evaluationFormKey,
b.evaluationFormName evaluationFormName,
a.evaluationFormPublished evaluationFormPublished,
a.evaluatorId evaluatorKey,
a.evaluationId evaluationKey,
a.mediaType mediaType,
a.neverRelease neverRelease,
from_utc_timestamp(a.releaseDate, trim(e.timeZone)) releaseDate,
a.status status,
a.queueId queueKey,
a.wrapUpCode
from gpc_hellofresh.dim_evaluations a, gpc_hellofresh.dim_evaluation_forms b,
    gpc_hellofresh.fact_evaluation_total_scores c,
 gpc_hellofresh.dim_routing_queues d, queue_mapping e
where a.evaluationFormId = b.evaluationFormId
and a.evaluationId = c.evaluationId
and a.conversationDatePart = c.conversationDatePart
and a.queueId = d.queueId
            and d.queueName = e.queueName
    """)

    return df
