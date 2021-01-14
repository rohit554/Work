from pyspark.sql import SparkSession


def fact_conversation_aggregate_metrics(spark: SparkSession, extract_date, extract_start_time, extract_end_time):
    agg_metrics = spark.sql(f"""
                                            select
	cast(intervalStart as date) date,
	intervalStart,
	intervalEnd,
	mediaType,
	messageType,
	originatingDirection,
	queueId,
	userId,
	wrapUpCode,
	nBlindTransferred_count nBlindTransferred,
	nConnected_count nConnected,
	nConsult_count nConsult,
	nConsultTransferred_count nConsultTransferred,
	nError_count nError,
	nOffered_count nOffered,
	nOutbound_count nOutbound,
	nOutboundAbandoned_count nOutboundAbandoned,
	nOutboundAttempted_count nOutboundAttempted,
	nOutboundConnected_count nOutboundConnected,
	nOverSla_count nOverSla,
	nStateTransitionError_count nStateTransitionError,
	nTransferred_count nTransferred,
	tAbandon_count nAbandon,
	tAbandon_sum tAbandon,
	tAcd_count nAcd,
	tAcd_sum tAcd,
	tAcw_count nAcw,
	tAcw_sum tAcw,
	tAgentResponseTime_count nAgentResponseTime,
	tAgentResponseTime_sum tAgentResponseTime,
	tAlert_count nAlert,
	tAlert_sum tAlert,
	tAnswered_count nAnswered,
	tAnswered_sum tAnswered,
	tContacting_count nContacting,
	tContacting_sum tContacting,
	tDialing_count nDialing,
	tDialing_sum tDialing,
	tFlowOut_count nFlowOut,
	tFlowOut_sum tFlowOut,
	tHandle_count nHandle,
	tHandle_sum tHandle,
	tHeld_count nHeld,
	tHeld_sum tHeld,
	tHeldComplete_count nHeldComplete,
	tHeldComplete_sum tHeldComplete,
	tIvr_count nIvr,
	tIvr_sum tIvr,
	tMonitoring_count nMonitoring,
	tMonitoring_sum tMonitoring,
	tNotResponding_count nNotResponding,
	tNotResponding_sum tNotResponding,
	tShortAbandon_count nShortAbandon,
	tShortAbandon_sum tShortAbandon,
	tTalk_count nTalk,
	tTalk_sum tTalk,
	tTalkComplete_count nTalkComplete,
	tTalkComplete_sum tTalkComplete,
	tUserResponseTime_count nUserResponseTime,
	tUserResponseTime_sum tUserResponseTime,
	tVoicemail_count nVoicemail,
	tVoicemail_sum tVoicemail,
	tWait_count nWait,
	tWait_sum tWait
from
	(
	select
		mediaType, messageType, originatingDirection, queueId, userId, wrapUpCode, cast(split(`interval`, "/")[0] as timestamp) intervalStart, cast(split(`interval`, "/")[1] as timestamp) intervalEnd, metric, `count`, `sum`
	from
		(
		select
			`group` .*, `interval`, metric, stats.*
		from
			(
			select
				`group`, `interval`, col.*
			from
				(
				select
					`group`, data.`interval`, explode(data.metrics)
				from
					(
					select
						group, explode(data) data
					from
					raw_conversation_aggregates
                    where 
                        extractIntervalStartTime = '{extract_start_time}'
                        and extractIntervalEndTime = '{extract_end_time}'
                        and  extractDate = '{extract_date}'
                        ) ) ) ) ) pivot ( sum(`count`) `count`,
	sum(`sum`) `sum` for metric in ("nBlindTransferred", "nConnected", "nConsult", "nConsultTransferred",
                                    "nError", "nOffered", "nOutbound", "nOutboundAbandoned", "nOutboundAttempted",
                                    "nOutboundConnected", "nOverSla", "nStateTransitionError", "nTransferred",
                                    "tAbandon", "tAcd", "tAcw", "tAgentResponseTime", "tAlert", "tAnswered",
                                    "tContacting", "tDialing", "tFlowOut", "tHandle", "tHeld", "tHeldComplete",
                                    "tIvr", "tMonitoring", "tNotResponding", "tShortAbandon", "tTalk", "tTalkComplete",
                                    "tUserResponseTime", "tVoicemail", "tWait") )
                                            """)

    agg_metrics.registerTempTable("conversation_agg_metrics")

    incorrect_interval_data = spark.sql(f"""
			select count(*) as count from conversation_agg_metrics 
      			where cast((intervalEnd - intervalStart) as string) != '15 minutes'
                        """).rdd.collect()[0]['count']
    if incorrect_interval_data > 0:
        raise Exception("Incorrect interval in aggregates - only 15 min intervals are allowed")
    dist_dates = spark.sql(
        "select distinct date, intervalStart, intervalEnd from conversation_agg_metrics")
    dist_dates.registerTempTable("dist_dates")
    spark.sql(f"""delete from fact_conversation_aggregate_metrics a where 
                                                exists (select 1 from dist_dates b
                                                    where b.`date` = a.`date`
                                                    and b.`intervalStart` = a.`intervalStart`
                                                    and b.`intervalEnd` = a.`intervalEnd`
                                                        )
                                """)
    spark.sql(
        "insert into fact_conversation_aggregate_metrics select * from conversation_agg_metrics")
