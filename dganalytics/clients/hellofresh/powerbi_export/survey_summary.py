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
            SELECT DISTINCT 
                a.surveyId,
                a.agentId,
                a.callType,
                from_utc_timestamp(a.surveyCompletionDate, trim(coalesce(e.timeZone, 'UTC'))) AS surveyCompletionDate,
                from_utc_timestamp(a.createdAt, trim(coalesce(e.timeZone, 'UTC'))) AS createdAt,
                a.email,
                a.externalRef,
                a.conversationId,
                a.conversationKey,
                a.queueKey,
                a.userKey,
                a.wrapUpCodeKey,
                a.restricted,
                from_utc_timestamp(a.surveySentDate, trim(coalesce(e.timeZone, 'UTC'))) AS surveySentDate,
                a.statusDescription,
                a.status,
                a.surveyTypeId,
                a.surveyType,
                from_utc_timestamp(a.updatedAt, trim(coalesce(e.timeZone, 'UTC'))) AS updatedAt,
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
                a.originatingDirection,
                b.participants[0].sessions[0].mediaType mediaType,
                b.participants[0].sessions[0].messageType messageType,
                e.region,
                cast(from_utc_timestamp(a.surveySentDate, trim(coalesce(e.timeZone, 'UTC'))) as DATE) AS surveySentDatePart
            FROM 
                sdx_hellofresh.dim_hellofresh_interactions a
                LEFT JOIN queue_mapping e ON trim(lower(a.callType)) = trim(lower(e.queueName))
                join gpc_hellofresh.raw_conversation_details b
                    ON a.conversationId = b.conversationId
                    AND cast(from_utc_timestamp(a.surveySentDate, trim(coalesce(e.timeZone, 'UTC'))) as DATE) >= date_sub(current_date(), 16)
                    AND extractDate >= date_sub(current_date(), 16)
    """)

    # Delete old data from the table for the last 15 days before inserting new data
    spark.sql(f"""
        DELETE FROM pbi_hellofresh.survey_summary 
        WHERE surveySentDatePart >= date_sub(current_date(), 16)
    """)

    # Write new data to the target table
    df.write.mode("append").saveAsTable("pbi_hellofresh.survey_summary")

    return spark.sql(f"""SELECT surveyId,
            agentId,
            callType,
            surveyCompletionDate,
            createdAt,
            email,
            externalRef,
            conversationId,
            conversationKey,
            queueKey,
            userKey,
            wrapUpCodeKey,
            restricted,
            surveySentDate,
            statusDescription,
            status,
            surveyTypeId,
            surveyType,
            updatedAt,
            respondentLanguage,
            country,
            agentEmail,
            agentGroup,
            contactChannel,
            wrapUpName,
            comments,
            OcsatAchieved,
            OcsatMax,
            improvement_categories,
            csatAchieved,
            csatMax,
            fcr,
            fcrMaxResponse,
            npsScore,
            npsMaxResponse,
            ces,
            cesMaxResponse,
            openText,
            selServerCsat,
            selServerCsatMaxResponse,
            usCsat,
            usCsatMaxResponse,
            originatingDirection,
            mediaType,
            messageType
    FROM pbi_hellofresh.survey_summary
                     WHERE surveySentDatePart >= add_months(current_date(), -12)
                     {"and region IN ('US', 'CA')" if region == 'US' else " " } """)