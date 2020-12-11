from dganalytics.utils.utils import get_path_vars
from pyspark.sql import SparkSession
import os
import pandas as pd


def export_conversion_metrics_daily_summary(spark: SparkSession, tenant: str, region: str):

    tenant_path, db_path, log_path = get_path_vars(tenant)
    '''
    queue_timezones = pd.read_csv(os.path.join(tenant_path, 'data',
                                               'config', 'Queue_TimeZone_Mapping.csv'), header=0)
        '''
    queue_timezones = pd.read_json(os.path.join(tenant_path, 'data',
                                                'config', 'Queue_TimeZone_Mapping.json'))
    queue_timezones = pd.DataFrame(queue_timezones['values'].tolist())
    header = queue_timezones.iloc[0]
    queue_timezones = queue_timezones[1:]
    queue_timezones.columns = header

    queue_timezones = spark.createDataFrame(queue_timezones)
    queue_timezones.registerTempTable("queue_timezones")

    df = spark.sql(f"""
            select 
	cast(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) as date) as emitDate, a.originatingDirection,
		'' purpose , a.userId userKey, a.mediaType, a.messageType, a.wrapUpCode wrapUpCodeKey, a.queueId queueKey,
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
		sum(round(tAbandon ,3)) as tAbandon,
		sum(nAbandon) as tAbandonCount,
		sum(round(tAcd ,3)) as tAcd,
		sum(nAcd) as tAcdCount,
		sum(round(tAcw ,3)) as tAcw,
		sum(nAcw) as tAcwCount,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTime,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTimeSessionCount_0,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTimeSessionCount_1_300,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTimeSessionCount_301_900,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTimeSessionCount_901_1800,
		sum(round(tAgentResponseTime,3)) as tAgentResponseTimeSessionCount_1801,
		sum(round(tAnswered ,3)) as tAnswered,
		sum(nAnswered) as tAnsweredCount,
		sum(round(tContacting ,3)) as tContacting ,
		sum(round(tDialing ,3)) as tDialing ,
		sum(round(tHandle ,3)) as tHandle ,
		sum(nHandle) as tHandleCount,
		sum(round(tHeldComplete ,3)) as tHeld,
		sum(nHeldComplete) as tHeldCount,
		sum(round(tHeldComplete ,3)) as tHeldComplete,
		sum(round(tIvr ,3)) as tIvr,
		sum(round(tNotResponding,3)) as tNotResponding,
		sum(round(tShortAbandon,3)) as tShortAbandon,
		sum(nShortAbandon) as tShortAbandonCount,
		sum(round(tTalkComplete,3)) as tTalk,
		sum(round(tTalkComplete,3)) as tTalkComplete,
		sum(round(tVoicemail,3)) as tVoicemail,
		sum(round(tWait,3)) as tWait,
		sum(nTalkComplete) as tTalkCompleteCount,
		sum(nHeldComplete) as tHeldCompleteCount
        from gpc_hellofresh.fact_conversation_aggregate_metrics a, gpc_hellofresh.dim_routing_queues b, queue_timezones c
        where a.queueId = b.queueId
            and b.queueName = c.queueName
			and c.region {" = 'US'" if region == 'US' else " <> 'US'"}
           group by
           cast(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) as date), a.originatingDirection,
           a.userId, a.mediaType, a.messageType, a.wrapUpCode , a.queueId 
    """)

    return df
