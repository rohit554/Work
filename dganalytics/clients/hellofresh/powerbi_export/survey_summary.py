from pyspark.sql import SparkSession
from dganalytics.utils.utils import get_path_vars
import os
import pandas as pd


def export_survey_summary(spark: SparkSession, tenant: str, region: str):
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
on trim(lower(a.callType)) = trim(lower(e.queueName))
    where e.region {" = 'US'" if region == 'US' else " <> 'US'"}
    """)

    return df
