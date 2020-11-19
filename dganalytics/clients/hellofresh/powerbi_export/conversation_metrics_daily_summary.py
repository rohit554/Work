from dganalytics.utils.utils import get_spark_session, export_powerbi_csv, get_path_vars
from pyspark.sql import SparkSession
from dganalytics.connectors.gpc.gpc_utils import pb_export_parser, get_dbname, gpc_utils_logger
import os
import pandas as pd


def export_conversion_metrics_daily_summary(spark: SparkSession, tenant: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    queue_timezones = pd.read_csv(os.path.join(tenant_path, 'data',
                                               'config', 'Queue_TimeZone_Mapping.csv'), header=0)
    # queue_mapping = spark.read.option("header", "true").csv(
    #    os.path.join('file:', tenant_path, 'data', 'config', 'Queue_TimeZone_Mapping.csv'))
    queue_timezones = spark.createDataFrame(queue_timezones)
    queue_timezones.registerTempTable("queue_timezones")

    df = spark.sql("""
            select 
	cast(from_utc_timestamp(a.emitDateTime, trim(c.timeZone)) as date) as emitDate, a.originatingDirection,
		'' purpose , a.agentId userKey, a.mediaType, a.messageType, a.wrapUpCode wrapUpCodeKey, a.queueId queueKey,
		sum(nBlindTransferred) as nBlindTransferred,
		sum(nConnected) as nConnected,
		sum(nConsult) as nConsult,
		sum(nConsultTransferred) as nConsultTransferred,
		sum(nError) as nError,
		sum(nOffered) as nOffered,
		sum(nOutbound) as nOutbound,
		sum(nOutboundAbandoned) as nOutboundAbandoned,
		sum(nOutboundAttempted) as nOutboundAttempted,
		sum(nOutboundConnected) as nOutboundConnected,
		sum(nOverSla) as nOverSla,
		sum(nTransferred) as nTransferred,
		sum(round(tAbandon ,3)*1000) as tAbandon,
		sum(nAbandon) as tAbandonCount,
		sum(round(tAcd ,3)*1000) as tAcd,
		sum(nAcd) as tAcdCount,
		sum(round(tAcw ,3)*1000) as tAcw,
		sum(nAcw) as tAcwCount,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTime,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTimeSessionCount_0,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTimeSessionCount_1_300,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTimeSessionCount_301_900,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTimeSessionCount_901_1800,
		sum(round(tAgentResponse,3)*1000) as tAgentResponseTimeSessionCount_1801,
		sum(round(tAnswered ,3)*1000) as tAnswered,
		sum(nAnswered) as tAnsweredCount,
		sum(round(tContacting ,3)*1000) as tContacting ,
		sum(round(tDialing ,3)*1000) as tDialing ,
		sum(round(tHandle ,3)*1000) as tHandle ,
		sum(nHandle) as tHandleCount,
		sum(round(tHeldComplete ,3)*1000) as tHeld,
		sum(nHeldComplete) as tHeldCount,
		sum(round(tHeldComplete ,3)*1000) as tHeldComplete,
		sum(round(tIvr ,3)*1000) as tIvr,
		sum(round(tNotResponding,3)*1000) as tNotResponding,
		sum(round(tShortAbandon,3)*1000) as tShortAbandon,
		sum(nShortAbandon) as tShortAbandonCount,
		sum(round(tTalkComplete,3)*1000) as tTalk,
		sum(round(tTalkComplete,3)*1000) as tTalkComplete,
		sum(round(tVoicemail,3)*1000) as tVoicemail,
		sum(round(tWait,3)*1000) as tWait,
		sum(nTalkComplete) as tTalkCompleteCount,
		sum(nHeldComplete) as tHeldCompleteCount
        from gpc_hellofresh.fact_conversation_metrics a, gpc_hellofresh.dim_routing_queues b, queue_timezones c
        where a.queueId = b.queueId 
            and b.queueName = c.queueName
           group by 
           cast(from_utc_timestamp(a.emitDateTime, trim(c.timeZone)) as date), a.originatingDirection,
           a.agentId, a.mediaType, a.messageType, a.wrapUpCode , a.queueId 
		   limit 100000
    """)

    return df
