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
		SELECT
			CAST(from_utc_timestamp(a.intervalStart, trim(c.timeZone)) AS date) AS emitDate,
			date_format(from_utc_timestamp(a.intervalStart, trim(c.timeZone)), 'HH:mm:ss') as intervalStart,
			date_format(from_utc_timestamp(a.intervalEnd, trim(c.timeZone)), 'HH:mm:ss') as intervalEnd,
			a.originatingDirection,
			'' purpose,
			a.userId userKey,
			a.mediaType,
			a.messageType,
			a.wrapUpCode wrapUpCodeKey,
			a.queueId queueKey,
			sum(nBlindTransferred) AS nBlindTransferred,
			sum(nConnected) AS nConnected,
			sum(nConsult) AS nConsult,
			sum(nConsultTransferred) AS nConsultTransferred,
			sum(nError) AS nError,
			sum(nOffered) AS nOffered,
			sum(nOutbound) AS nOutbound,
			sum(nOutboundAbandoned) AS nOutboundAbandoned,
			sum(nOutboundAttempted) AS nOutboundAttempted,
			sum(nOutboundConnected) AS nOutboundConnected,
			sum(nOverSla) AS nOverSla,
			sum(nTransferred) AS nTransferred,
			sum(round(tAbandon , 3)) AS tAbandon,
			sum(nAbandon) AS tAbandonCount,
			sum(round(tAcd , 3)) AS tAcd,
			sum(nAcd) AS tAcdCount,
			sum(round(tAcw , 3)) AS tAcw,
			sum(nAcw) AS tAcwCount,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTime,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTimeSessionCount_0,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTimeSessionCount_1_300,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTimeSessionCount_301_900,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTimeSessionCount_901_1800,
			sum(round(tAgentResponseTime, 3)) AS tAgentResponseTimeSessionCount_1801,
			sum(round(tAnswered , 3)) AS tAnswered,
			sum(nAnswered) AS tAnsweredCount,
			sum(round(tContacting , 3)) AS tContacting,
			sum(round(tDialing , 3)) AS tDialing,
			sum(round(tHandle , 3)) AS tHandle,
			sum(nHandle) AS tHandleCount,
			sum(round(tHeldComplete , 3)) AS tHeld,
			sum(nHeldComplete) AS tHeldCount,
			sum(round(tHeldComplete , 3)) AS tHeldComplete,
			sum(round(tIvr , 3)) AS tIvr,
			sum(round(tNotResponding, 3)) AS tNotResponding,
			sum(round(tShortAbandon, 3)) AS tShortAbandon,
			sum(nShortAbandon) AS tShortAbandonCount,
			sum(round(tTalkComplete, 3)) AS tTalk,
			sum(round(tTalkComplete, 3)) AS tTalkComplete,
			sum(round(tVoicemail, 3)) AS tVoicemail,
			sum(round(tWait, 3)) AS tWait,
			sum(nTalkComplete) AS tTalkCompleteCount,
			sum(nHeldComplete) AS tHeldCompleteCount
		FROM
			gpc_hellofresh.fact_conversation_aggregate_metrics a,
			gpc_hellofresh.dim_routing_queues b,
			queue_timezones c
		WHERE
			a.queueId = b.queueId
			AND b.queueName = c.queueName
			AND c.region {" = 'US'" IF region == 'US' ELSE " <> 'US'" }
		GROUP BY
			a.originatingDirection,
			a.userId,
			a.mediaType,
			a.messageType,
			a.wrapUpCode ,
			a.queueId,
			a.intervalStart,
			a.intervalEnd
    """)

    return df
