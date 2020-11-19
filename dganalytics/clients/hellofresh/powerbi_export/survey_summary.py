from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_survey_summary(spark: SparkSession, tenant: str):
    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_mapping = pd.read_csv(os.path.join(tenant_path, 'data',
                                             'config', 'Queue_TimeZone_Mapping.csv'), header=0)
    # queue_mapping = spark.read.option("header", "true").csv(
    #    os.path.join('file:', tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping.csv'))
    queue_mapping = spark.createDataFrame(queue_mapping)
    queue_mapping.registerTempTable("queue_mapping")

    df = spark.sql("""
           select 
                a.surveyId,
                a.agentId,
                a.callType,
                from_utc_timestamp(a.surveyCompletionDate, trim(coalesce(e.timeZone, 'UTC'))) surveyCompletionDate,
                from_utc_timestamp(a.createdAt, trim(coalesce(e.timeZone, 'UTC'))) createdAt,
                a.email,
                a.externalRef,
                a.conversationId,
                a.conversationKey,
                a.queueKey,
                a.userKey,
                a.mediaType,
                a.wrapUpCodeKey,
                a.restricted,
                from_utc_timestamp(a.surveySentDate, trim(coalesce(e.timeZone, 'UTC'))) surveySentDate,
                a.statusDescription,
                a.status,
                a.surveyTypeId,
                a.surveyType,
                from_utc_timestamp(a.updatedAt, trim(coalesce(e.timeZone, 'UTC'))) updatedAt,
                a.respondentLanguage,
                a.country,
                a.agentEmail,
                a.agentGroup,
                a.contactChannel,
                a.wrapUpName,
                a.comments,
                a.OcsatAchieved,
                a.OcsatMax,
                a.improvement_categories,
                a.csatAchieved,
                a.csatMax,
                a.fcr,
                a.fcrMaxResponse,
                a.npsScore,
                a.npsMaxResponse,
                a.ces,
                a.cesMaxResponse,
                a.openText,
                a.selServerCsat,
                a.selServerCsatMaxResponse,
                a.usCsat,
                a.usCsatMaxResponse,
                a.originatingDirection
from sdx_hellofresh.dim_hellofresh_interactions a left join queue_mapping e
where trim(lower(a.callType)) = trim(lower(e.queueName))
    """)

    return df
